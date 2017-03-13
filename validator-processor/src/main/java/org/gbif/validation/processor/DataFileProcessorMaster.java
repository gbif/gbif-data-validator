package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.MetadataEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.evaluator.EvaluationChain;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.evaluator.ResourceConstitutionEvaluationChain;
import org.gbif.validation.evaluator.runner.MetadataEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordCollectionEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordEvaluatorRunner;
import org.gbif.validation.jobserver.messages.DataJob;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import org.apache.commons.lang3.mutable.MutableInt;

import static org.gbif.validation.api.model.ValidationProfile.GBIF_INDEXING_PROFILE;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Akka actor that acts as the central coordinator of parallel data processing.
 * This class decides if the data input should be split into smaller pieces to be processed by worker actors.
 */
public class DataFileProcessorMaster extends AbstractLoggingActor {

  private static final int MAX_WORKER = Runtime.getRuntime().availableProcessors() * 50;

  private final Map<Term, TabularDataFile> rowTypeDataFile;
  private final Map<Term, CollectorGroupProvider> rowTypeCollectorProviders;
  private final Map<Term, List<CollectorGroup>> rowTypeCollectors;
  private final Collection<ValidationResultElement> validationResultElements;
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;

  //current working directory for the current validation
  private File workingDir;

  /**
   * Full constructor.
   */
  DataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectorProviders = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);
    validationResultElements = new ConcurrentLinkedQueue<>();

    receive(
            //this should only be called once
            match(DataJob.class, dataJobMessage -> {
              dataJob = (DataJob<DataFile>)dataJobMessage;
              workingDir = new File(baseWorkingDir, UUID.randomUUID().toString());
              workingDir.mkdir();
              processDataFile(factory, fileSplitSize);
            })
              .match(DataWorkResult.class, this::processRecordBasedResults)
              .match(MetadataWorkResult.class, this::processMetadataBasedResults).build()
    );
  }

  /**
   * Starting point of the entire process to evaluate a {@link DataFile}.
   *
   * @param factory
   * @param fileSplitSize
   * @throws IOException
   */
  private void processDataFile(EvaluatorFactory factory, Integer fileSplitSize) throws IOException {
    DataFile dataFile = dataJob.getJobData();

    StructuralEvaluationResult structuralEvaluationResult = evaluateResourceIntegrityAndStructure(dataFile, factory);
    if(structuralEvaluationResult.canContinueEvaluation()){
      DwcDataFile dwcDataFile = structuralEvaluationResult.dwcDataFile;

      init(dwcDataFile.getTabularDataFiles());

      EvaluationChain.Builder evaluationChainBuilder =
              EvaluationChain.Builder.using(dwcDataFile, factory)
                      .evaluateMetadataContent()
                      .evaluateCoreUniqueness()
                      .evaluateReferentialIntegrity()
                      .evaluateChecklist();

      //numOfWorkers is used to know how many responses we are expecting
      final MutableInt numOfWorkers = new MutableInt(0);
      prepareRecordBasedEvaluations(dwcDataFile, fileSplitSize, evaluationChainBuilder, numOfWorkers);

      EvaluationChain evaluationChain = evaluationChainBuilder.build();
      numOfWorkers.add(evaluationChain.getNumberOfRowTypeEvaluationUnits());
      numOfWorkers.add(evaluationChain.getNumberOfMetadataEvaluationUnits());

      this.numOfWorkers = numOfWorkers.intValue();

      log().info("Expected {} worker response(s)", numOfWorkers);
      log().info(evaluationChain.toString());

      //now trigger everything
      startAllActors(evaluationChain);
    }
  }

  /**
   * Initialize all member variables based on the list of {@link DataFile}.
   * @param dataFiles
   */
  private void init(Iterable<TabularDataFile> dataFiles){
    dataFiles.forEach(df -> {
      rowTypeCollectors.putIfAbsent(df.getRowType(), new ArrayList<>());
      rowTypeDataFile.put(df.getRowType(), df);
      List<Term> columns = Arrays.asList(df.getColumns());
      rowTypeCollectorProviders.put(df.getRowType(), new CollectorGroupProvider(df.getRowType(), columns));
    });
  }

  /**
   * Calculates the required splits to process all the {@link DataFile}.
   * This method expect {@link #init(Iterable)} to be already called.
   *
   * @param dwcDataFile     all files to evaluate. For DarwinCore it also includes all extensions.
   * @param fileSplitSize
   * @param evaluationChainBuilder current EvaluationChain.Builder
   * @param numOfWorkers
   *
   * @return
   */
  private void prepareRecordBasedEvaluations(final DwcDataFile dwcDataFile, final Integer fileSplitSize,
                                             final EvaluationChain.Builder evaluationChainBuilder, final MutableInt numOfWorkers) {
    dwcDataFile.getTabularDataFiles().forEach(df -> {
      List<Term> columns = Arrays.asList(df.getColumns());
      try {
        List<TabularDataFile> splitDataFile = DataFileSplitter.splitDataFile(df, fileSplitSize, workingDir.toPath());
        numOfWorkers.add(splitDataFile.size());
        evaluationChainBuilder.evaluateRecords(df.getRowType(), columns, df.getDefaultValues(), splitDataFile);
      } catch (IOException ioEx) {
        log().error("Failed to split data", ioEx);
      }
    });
  }

  /**
   * Evaluate the structure of the resource represented by the provided {@link DataFile}.
   * If a RESOURCE_INTEGRITY issue is found, the JobStatusResponse will be emitted and this actor will be stopped.
   * FIXME this should run outside the Actor
   * @param dataFile
   * @return is the resource integrity allows to continue the evaluation or not
   */
  private StructuralEvaluationResult evaluateResourceIntegrityAndStructure(DataFile dataFile, EvaluatorFactory factory) {

    ResourceConstitutionEvaluationChain evaluationChain = ResourceConstitutionEvaluationChain.Builder.using(dataFile, factory).build();
    Optional<List<ValidationResultElement>> validationResultElementList = evaluationChain.runResourceStructureEvaluator();

    if (validationResultElementList.isPresent()) {
      //check if we have an issue that requires to stop the evaluation process
      if (evaluationChain.evaluationStopped()) {
        emitResponseAndStop(new JobStatusResponse<>(JobStatus.FINISHED, dataJob.getJobId(),
                new ValidationResult(false, dataFile.getSourceFileName(), dataFile.getFileFormat(),
                        GBIF_INDEXING_PROFILE, validationResultElementList.get())));
        return StructuralEvaluationResult.createStopEvaluation();
      }
      validationResultElements.addAll(validationResultElementList.get());
    }

    // try to prepare the DataFile
    boolean stopEvaluation = false;
    DwcDataFile dwcDataFile = null;
    try {
      dwcDataFile = DataFileFactory.prepareDataFile(dataFile, workingDir.toPath());
    } catch (IOException ioEx){
      emitErrorAndStop(dataFile, ValidationErrorCode.IO_ERROR, null);
      stopEvaluation = true;
    } catch(UnsupportedDataFileException ex) {
      emitErrorAndStop(dataFile, ValidationErrorCode.UNSUPPORTED_FILE_FORMAT, ex.getMessage());
      stopEvaluation = true;
    }
    return new StructuralEvaluationResult(stopEvaluation, dwcDataFile);
  }

  /**
   * Triggers processing of all metadataEvaluator and rowTypeEvaluation from the chain.
   * @param evaluationChain
   */
  private void startAllActors(EvaluationChain evaluationChain) {
    MetadataEvaluatorRunner metadataEvaluatorRunner = (dwcDataFile, metadataEvaluator) -> {
      ActorRef actor = createSingleActor(metadataEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runMetadataContentEvaluation(metadataEvaluatorRunner);

    RecordCollectionEvaluatorRunner runner = (dwcDataFile, rowType, recordCollectionEvaluator) -> {
      ActorRef actor = createSingleActor(rowType, recordCollectionEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runRowTypeEvaluation(runner);

    RecordEvaluatorRunner recordEvaluatorRunner = (dataFiles, rowType, recordEvaluator) -> {
      ActorRef workerRouter = createWorkerRoutes(Math.min(dataFiles.size(), MAX_WORKER),
              rowType, recordEvaluator);
      dataFiles.forEach(dataFile -> workerRouter.tell(dataFile, self()));
    };
    evaluationChain.runRecordEvaluation(recordEvaluatorRunner);
  }

  /**
   * Creates the worker router using the calculated number of workers.
   */
  private ActorRef createWorkerRoutes(int poolSize, Term rowType, RecordEvaluator recordEvaluator) {
    log().info("Created routes for " + rowType);
    String actorName = "dataFileRouter_" + UUID.randomUUID();
    return getContext().actorOf(
            new RoundRobinPool(poolSize).props(Props.create(DataFileRecordsActor.class,
                    recordEvaluator, rowTypeCollectorProviders.get(rowType))),
            actorName);
  }

  /**
   * Creates an Actor for the provided {@link Term} and {@link RecordCollectionEvaluator}.
   */
  private ActorRef createSingleActor(MetadataEvaluator metadataEvaluator) {
    String actorName =  "MetadataEvaluatorActor_" + UUID.randomUUID();
    return getContext().actorOf(Props.create(MetadataContentActor.class, metadataEvaluator), actorName);
  }

  /**
   * Creates an Actor for the provided {@link EvaluationChain.RowTypeEvaluationUnit}.
   */
  private ActorRef createSingleActor(Term rowType, RecordCollectionEvaluator recordCollectionEvaluator) {
    String actorName =  "RowTypeEvaluationUnitActor_" + UUID.randomUUID();
    return getContext().actorOf(Props.create(DataFileRowTypeActor.class,
            rowType, recordCollectionEvaluator, rowTypeCollectorProviders.get(rowType)), actorName);
  }

  /**
   * Called when a single worker finished its work.
   * This can represent en entire file or a part of a file
   */
  private void processRecordBasedResults(DataWorkResult result) {

    //FIXME
    if(DataWorkResult.Result.FAILED == result.getResult()) {
      log().error("DataWorkResult = FAILED: {}", result);
    }
    collectResult(result);
    incrementWorkerCompleted();
  }

  private void processMetadataBasedResults(MetadataWorkResult result) {
    if(DataWorkResult.Result.SUCCESS == result.getResult()) {
      result.getValidationResultElements().ifPresent(validationResultElements::addAll);
    }
    incrementWorkerCompleted();
  }

  /**
   * Once a response is received, increment counter and check for completion.
   */
  private void incrementWorkerCompleted() {
    int numberOfWorkersCompleted = workerCompleted.incrementAndGet();
    log().info("Got {} worker response(s)", numberOfWorkersCompleted);

    if (numberOfWorkersCompleted == numOfWorkers) {
      emitResponseAndStop(new JobStatusResponse<>(JobStatus.FINISHED, dataJob.getJobId(), buildResult()));
    }
  }

  /**
   * Collects individual results and aggregates them in the internal data structures.
   */
  private void collectResult(DataWorkResult result) {
    rowTypeCollectors.compute(result.getRowType(), (key, val) -> {
      val.add(result.getCollectors());
      return val;
    });
  }

  /**
   * Builds and merges the ValidationResult from the aggregated data.
   */
  private ValidationResult buildResult() {
    List<ValidationResultElement> resultElements = new ArrayList<>();
    rowTypeCollectors.forEach((rowType, collectorList) -> resultElements.add(
                                                            CollectorGroup.mergeAndGetResult(
                                                            rowTypeDataFile.get(rowType),
                                                            rowTypeDataFile.get(rowType).getSourceFileName(),
                                                            collectorList)
    ));

    //merge all ValidationResultElement into those collected by rowType
    validationResultElements.forEach(
            vre -> {
              Optional<ValidationResultElement> currentElement = resultElements.stream()
                      .filter(re -> vre.getFileName().equals(vre.getFileName()))
                      .findFirst();

              if (currentElement.isPresent()) {
                currentElement.get().getIssues().addAll(vre.getIssues());
              } else {
                resultElements.add(vre);
              }
            }
    );

    return new ValidationResult(true, dataJob.getJobData().getSourceFileName(), dataJob.getJobData().getFileFormat(),
            GBIF_INDEXING_PROFILE, resultElements);
  }


  /**
   * Emit the provided response to the parent Actor and stop this Actor.
   */
  private void emitResponseAndStop(JobStatusResponse<?> response) {
    context().parent().tell(response, self());
    deleteWorkingDir();
    getContext().stop(self());
  }

  private void emitErrorAndStop(DataFile dataFile, ValidationErrorCode errorCode, String errorMessage) {
    JobStatusResponse<?> response = new JobStatusResponse<>(JobStatus.FAILED, dataJob.getJobId(),
            ValidationResult.onError(dataFile.getSourceFileName(), dataFile.getFileFormat(), errorCode, errorMessage));
    context().parent().tell(response, self());
    deleteWorkingDir();
    getContext().stop(self());
  }

  /**
   * Deletes the working directory if it exists.
   */
  private void deleteWorkingDir() {
    if (workingDir.exists()) {
      FileUtils.deleteDirectoryRecursively(workingDir);
    }
  }

  private static class StructuralEvaluationResult {
    private boolean stopEvaluation;
    private DwcDataFile dwcDataFile;

    static StructuralEvaluationResult createStopEvaluation(){
      return new StructuralEvaluationResult(true, null);
    }

    StructuralEvaluationResult(boolean stopEvaluation, DwcDataFile dwcDataFile){
      this.stopEvaluation = stopEvaluation;
      this.dwcDataFile = dwcDataFile;
    }

    public boolean canContinueEvaluation(){
      return !stopEvaluation;
    }
  }
}
