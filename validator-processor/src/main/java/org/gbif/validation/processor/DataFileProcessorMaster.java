package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.messages.DataJob;
import org.gbif.validation.source.DataFileFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
  private final List<ValidationResultElement> resourceStructureResultElement;
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
    resourceStructureResultElement = new ArrayList<>();

    receive(
            //this should only be called once
            match(DataJob.class, dataJobMessage -> {
              dataJob = (DataJob<DataFile>)dataJobMessage;
              workingDir = new File(baseWorkingDir, UUID.randomUUID().toString());
              workingDir.mkdir();
              processDataFile(factory, fileSplitSize);
            })
              .match(DataWorkResult.class, this::processResults).build()
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

    //Probably from here DataFile -> PreparedDataFile with list of derived tabular files

    if(evaluateResourceIntegrityAndStructure(dataFile, factory)){
      DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dataFile, workingDir.toPath());

      init(dwcDataFile.getTabularDataFiles());

      EvaluationChain.Builder evaluationChainBuilder =
              EvaluationChain.Builder.using(dwcDataFile, factory)
                      .evaluateCoreUniqueness()
                      .evaluateReferentialIntegrity();
                     // .evaluateChecklist(); fix UUID for folder name

      //numOfWorkers is used to know how many responses we are expecting
      final MutableInt numOfWorkers = new MutableInt(0);
      prepareRecordBasedEvaluations(dwcDataFile, fileSplitSize, evaluationChainBuilder, numOfWorkers);

      EvaluationChain evaluationChain = evaluationChainBuilder.build();
      numOfWorkers.add(evaluationChain.getNumberOfRowTypeEvaluationUnits());

      this.numOfWorkers = numOfWorkers.intValue();

      log().info("Expected {} worker response(s)", numOfWorkers);
      log().info(evaluationChain.toString());

      //now trigger everything
      processRowTypeEvaluationUnit(evaluationChain);
      processEvaluationUnit(evaluationChain);
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
   *
   * @param dataFile
   * @return is the resource integrity allows to continue the evaluation or not
   */
  private boolean evaluateResourceIntegrityAndStructure(DataFile dataFile, EvaluatorFactory factory) {
    Optional<List<ValidationResultElement>> validationResultElements =
            factory.createResourceStructureEvaluator(dataFile.getFileFormat())
                    .evaluate(dataFile);

    if (validationResultElements.isPresent()) {
      List<ValidationResultElement> validationResultElementList = new ArrayList<>(validationResultElements.get());

      //check if we have an issue that requires to stop the evaluation process
      if (validationResultElements.get().stream()
              .filter(vre -> vre.contains(EvaluationCategory.RESOURCE_INTEGRITY))
              .findAny().isPresent()) {
        emitResponseAndStop(new JobStatusResponse<>(JobStatus.FINISHED, dataJob.getJobId(),
                new ValidationResult(false, dataFile.getSourceFileName(), dataFile.getFileFormat(),
                        GBIF_INDEXING_PROFILE, validationResultElementList, null)));
        return false;
      } else {
        resourceStructureResultElement.addAll(validationResultElementList);
      }
    }
    return true;
  }

  /**
   * Triggers processing of all rowTypeEvaluationUnit in the {@link EvaluationChain}.
   * @param evaluationChain
   */
  private void processRowTypeEvaluationUnit(EvaluationChain evaluationChain) {
    evaluationChain.runRowTypeEvaluation( rtEvaluator -> {
      ActorRef actor = createSingleActor(rtEvaluator);
      actor.tell(rtEvaluator.getDataFile(), self());
    });
  }

  /**
   * Triggers processing of all recordEvaluationUnit in the {@link EvaluationChain}.
   * Creates a Actor/Worker for each evaluation unit.
   */
  private void processEvaluationUnit(EvaluationChain evaluationChain) {
    evaluationChain.runRecordEvaluation( rtEvaluator -> {
      ActorRef workerRouter = createWorkerRoutes(rtEvaluator);
      rtEvaluator.getDataFiles().forEach(dataFile -> workerRouter.tell(dataFile, self()));
    });
  }

  /**
   * Creates the worker router using the calculated number of workers.
   */
  private ActorRef createWorkerRoutes(EvaluationChain.RecordEvaluationUnit evaluationUnit) {
    log().info("Created routes for " + evaluationUnit.getRowType());
    String actorName = "dataFileRouter_" + UUID.randomUUID();
    return getContext().actorOf(
            new RoundRobinPool(Math.min(evaluationUnit.getDataFiles().size(), MAX_WORKER)).props(Props.create(DataFileRecordsActor.class,
                    evaluationUnit.getRecordEvaluator(),
                    rowTypeCollectorProviders.get(evaluationUnit.getRowType()))),
            actorName);
  }

  /**
   * Creates an Actor for the provided {@link EvaluationChain.RowTypeEvaluationUnit}.
   */
  private ActorRef createSingleActor(EvaluationChain.RowTypeEvaluationUnit rowTypeEvaluationUnit) {
    String actorName =  "RowTypeEvaluationUnitActor_" + UUID.randomUUID();
      return getContext().actorOf(Props.create(DataFileRowTypeActor.class,
              rowTypeEvaluationUnit.getRowType(),
              rowTypeEvaluationUnit.getRecordCollectionEvaluator(),
              rowTypeCollectorProviders.get(rowTypeEvaluationUnit.getRowType())), actorName);
  }

  /**
   * Called when a single worker finished its work.
   * This can represent en entire file or a part of a file
   */
  private void processResults(DataWorkResult result) {

    //FIXME
    if(DataWorkResult.Result.FAILED == result.getResult()) {
      log().error("DataWorkResult = FAILED: {}", result);
    }

    int numberOfWorkersCompleted = workerCompleted.incrementAndGet();
    log().info("Got {} worker response(s)", numberOfWorkersCompleted);

    collectResult(result);

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
   * Builds the ValidationResult from the aggregated data.
   */
  private ValidationResult buildResult() {
    List<ValidationResultElement> validationResultElements = new ArrayList<>();
    rowTypeCollectors.forEach((rowType, collectorList) -> validationResultElements.add(
                                                            CollectorGroup.mergeAndGetResult(
                                                            rowTypeDataFile.get(rowType),
                                                            rowTypeDataFile.get(rowType).getSourceFileName(),
                                                            collectorList)
    ));

    //FIXME results should be merged
    validationResultElements.addAll(resourceStructureResultElement);

    return new ValidationResult(true, dataJob.getJobData().getSourceFileName(), dataJob.getJobData().getFileFormat(),
            GBIF_INDEXING_PROFILE, validationResultElements, null);
  }

  /**
   * Emit the provided response to the parent Actor and stop this Actor.
   */
  private void emitResponseAndStop(JobStatusResponse<?> response) {
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
}
