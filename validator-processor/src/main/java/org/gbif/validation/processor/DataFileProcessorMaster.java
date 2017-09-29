package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.JobDataOutput;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationDataOutput;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.evaluator.DwcDataFileSupplier;
import org.gbif.validation.evaluator.EvaluationChain;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.evaluator.ResourceConstitutionEvaluationChain;
import org.gbif.validation.evaluator.runner.DwcDataFileEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordCollectionEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordEvaluatorRunner;
import org.gbif.validation.jobserver.messages.DataJob;
import org.gbif.validation.source.DataFileFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

  private final Map<RowTypeKey, TabularDataFile> rowTypeDataFile;
  private final Map<RowTypeKey, CollectorGroupProvider> rowTypeCollectorProviders;
  private final Map<RowTypeKey, List<CollectorGroup>> rowTypeCollectors;
  private final Collection<ValidationResultElement> validationResultElements;
  private final boolean preserveTemporaryFiles;
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;

  //current working directory for the current validation
  private File workingDir;

  /**
   * Full constructor.
   */
  DataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir,
                          boolean preserveTemporaryFiles) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectorProviders = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);
    validationResultElements = new ConcurrentLinkedQueue<>();
    this.preserveTemporaryFiles = preserveTemporaryFiles;

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
   * Creates Actor {@link Props}.
   * @param factory
   * @param fileSplitSize
   * @param baseWorkingDir
   * @param preserveTemporaryFiles
   * @return
   */
  public static Props createProps(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir,
                            boolean preserveTemporaryFiles) {
    return Props.create(DataFileProcessorMaster.class, factory, fileSplitSize, baseWorkingDir, preserveTemporaryFiles);
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
              EvaluationChain.Builder.using(dwcDataFile, factory, workingDir.toPath())
                      .evaluateMetadataContent()
                      .evaluateCoreUniqueness()
                      .evaluateReferentialIntegrity()
                      .evaluateChecklist();

      //numOfWorkers is used to know how many responses we are expecting
      final MutableInt numOfWorkers = new MutableInt(0);
      prepareRecordBasedEvaluations(dwcDataFile, fileSplitSize, evaluationChainBuilder, numOfWorkers);

      EvaluationChain evaluationChain = evaluationChainBuilder.build();
      numOfWorkers.add(evaluationChain.getNumberOfRowTypeEvaluationUnits());
      numOfWorkers.add(evaluationChain.getNumberOfDwcDataFileEvaluationUnits());

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
      rowTypeCollectors.putIfAbsent(df.getRowTypeKey(), new ArrayList<>());
      rowTypeDataFile.put(df.getRowTypeKey(), df);
      List<Term> columns = Arrays.asList(df.getColumns());
      rowTypeCollectorProviders.put(df.getRowTypeKey(), new CollectorGroupProvider(df.getRowTypeKey().getRowType(), columns));
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
        evaluationChainBuilder.evaluateRecords(df.getRowTypeKey(), df.getRecordIdentifier().orElse(null), columns,
                df.getDefaultValues().orElse(null), splitDataFile);
      } catch (IOException ioEx) {
        log().error("Failed to split data", ioEx);
      }
    });
  }

  /**
   * Evaluate the structure of the resource represented by the provided {@link DataFile}.
   * If a RESOURCE_INTEGRITY issue is found, the JobStatusResponse will be emitted and this actor will be stopped.
   * @param dataFile
   * @return is the resource integrity allows to continue the evaluation or not
   */
  private StructuralEvaluationResult evaluateResourceIntegrityAndStructure(DataFile dataFile, EvaluatorFactory factory) {

    DwcDataFileSupplier transformer = () -> DataFileFactory.prepareDataFile(dataFile, workingDir.toPath());
    ResourceConstitutionEvaluationChain evaluationChain =
            ResourceConstitutionEvaluationChain.Builder.using(dataFile, factory)
                    .transformedBy(transformer)
                    .evaluateDwcDataFile(factory.createPrerequisiteEvaluator())
                    .build();

    Optional<List<ValidationResultElement>> validationResultElementList = evaluationChain.run();
    if (validationResultElementList.isPresent()) {
      //check if we have an issue that requires to stop the evaluation process
      if (evaluationChain.evaluationStopped()) {
        emitResponseAndStop(buildJobStatusResponse(false, JobStatus.FINISHED, dataFile, validationResultElementList.get()));
        return StructuralEvaluationResult.createStopEvaluation();
      }
      mergeIssuesOnFilename(validationResultElementList.get(), validationResultElements);

      context().parent()
              .tell(buildJobStatusResponse(null, JobStatus.RUNNING, dataFile, new ArrayList<>(validationResultElements)), self());
    }

    return new StructuralEvaluationResult(false, evaluationChain.getTransformedDataFile());
  }

  /**
   * Triggers processing of all metadataEvaluator and rowTypeEvaluation from the chain.
   *
   * @param evaluationChain
   */
  private void startAllActors(EvaluationChain evaluationChain) {
    DwcDataFileEvaluatorRunner dwcDataFileEvaluatorRunner = (dwcDataFile, dwcDataFileEvaluator) -> {
      ActorRef actor = createSingleActor(dwcDataFileEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runDwcDataFileEvaluation(dwcDataFileEvaluatorRunner);

    RecordCollectionEvaluatorRunner runner = (dwcDataFile, rowTypeKey, recordCollectionEvaluator) -> {
      ActorRef actor = createSingleActor(rowTypeKey, recordCollectionEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runRecordCollectionEvaluation(runner);

    RecordEvaluatorRunner recordEvaluatorRunner = (dataFiles, rowTypeKey, recordEvaluator) -> {
      ActorRef workerRouter = createWorkerRoutes(Math.min(dataFiles.size(), MAX_WORKER),
              rowTypeKey, recordEvaluator);
      dataFiles.forEach(dataFile -> workerRouter.tell(dataFile, self()));
    };
    evaluationChain.runRecordEvaluation(recordEvaluatorRunner);
  }

  /**
   * Creates the worker router using the calculated number of workers.
   *
   */
  private ActorRef createWorkerRoutes(int poolSize, RowTypeKey rowTypeKey, RecordEvaluator recordEvaluator) {
    log().info("Created routes for " + rowTypeKey);
    String actorName = "dataFileRouter_" + UUID.randomUUID();
    return getContext().actorOf(
            new RoundRobinPool(poolSize).props(Props.create(DataFileRecordsActor.class,
                    recordEvaluator, rowTypeCollectorProviders.get(rowTypeKey))),
            actorName);
  }

  /**
   * Creates an Actor for the provided {@link Term} and {@link RecordCollectionEvaluator}.
   */
  private ActorRef createSingleActor(DwcDataFileEvaluator metadataEvaluator) {
    String actorName =  "MetadataEvaluatorActor_" + UUID.randomUUID();
    return getContext().actorOf(Props.create(MetadataContentActor.class, metadataEvaluator), actorName);
  }

  /**
   * Creates an Actor for the provided {@link EvaluationChain.RowTypeEvaluationUnit}.
   */
  private ActorRef createSingleActor(RowTypeKey rowTypeKey, RecordCollectionEvaluator recordCollectionEvaluator) {
    String actorName =  "RowTypeEvaluationUnitActor_" + UUID.randomUUID();
    return getContext().actorOf(Props.create(DataFileRowTypeActor.class,
            rowTypeKey, recordCollectionEvaluator, rowTypeCollectorProviders.get(rowTypeKey)), actorName);
  }

  /**
   * Called when a single worker finished its work.
   * This can represent en entire file or a part of a file
   */
  private void processRecordBasedResults(DataWorkResult result) {
    if(DataWorkResult.Result.FAILED == result.getResult()) {
      log().error("DataWorkResult = FAILED: {}", result);
      validationResultElements.add(ValidationResultElement.onException(
              result.getFileName(),
              EvaluationType.UNREADABLE_SECTION_ERROR, ""));
    }

    if(result.getRowTypeKey() == null) {
      log().error("RowTypeKey shall never be null: " +  result + ", " + result.getFileName()
      + ", " + result.getCollectors());
    }

    collectResult(result);
    incrementWorkerCompleted();
  }

  /**
   *
   * @param result
   */
  private void processMetadataBasedResults(MetadataWorkResult result) {
    log().info("Got MetadataWorkResult worker response(s)");
    if (DataWorkResult.Result.SUCCESS == result.getResult()) {
      result.getValidationResultElements().ifPresent(ver ->
              mergeIssuesOnFilename(ver, validationResultElements));
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
      ValidationResult validationResult = buildResult();
      emitDataOutput(buildJobDataOutput(validationResult));
      emitResponseAndStop(new JobStatusResponse<>(JobStatus.FINISHED, dataJob.getJobId(),
              dataJob.getStartTimeStamp(), dataJob.getJobData().getKey(),validationResult));
    }
  }

  /**
   * Collects individual results and aggregates them in the internal data structures.
   */
  private void collectResult(DataWorkResult result) {
    rowTypeCollectors.compute(result.getRowTypeKey(), (key, val) -> {
        val.add(result.getCollectors());
      return val;
    });
  }

  /**
   * Builds and merges the ValidationResult from the aggregated data.
   */
  private ValidationResult buildResult() {
    List<ValidationResultElement> resultElements = new ArrayList<>();
    rowTypeCollectors.forEach((rowTypeKey, collectorList) -> resultElements.add(
                                                            CollectorGroup.mergeAndGetResult(
                                                            rowTypeDataFile.get(rowTypeKey),
                                                            rowTypeDataFile.get(rowTypeKey).getSourceFileName(),
                                                            collectorList)
    ));

    //merge all ValidationResultElement into those collected by rowType
    mergeIssuesOnFilename(validationResultElements, resultElements);

    return new ValidationResult(true, dataJob.getJobData().getSourceFileName(), dataJob.getJobData().getFileFormat(),
            dataJob.getJobData().getReceivedAsMediaType(), GBIF_INDEXING_PROFILE, resultElements);
  }

  /**
   * Build a new {@link JobStatusResponse} based on the provided data.
   * @param indexable
   * @param status
   * @param dataFile
   * @param validationResultElement
   * @return
   */
  private JobStatusResponse<?> buildJobStatusResponse(Boolean indexable, JobStatus status, DataFile dataFile,
                                                      List<ValidationResultElement> validationResultElement) {
    return new JobStatusResponse<>(status, dataJob.getJobId(), dataJob.getStartTimeStamp(), dataFile.getKey(),
            new ValidationResult(indexable, dataFile.getSourceFileName(), dataFile.getFileFormat(),
                    dataFile.getReceivedAsMediaType(), GBIF_INDEXING_PROFILE, validationResultElement));
  }

  /**
   * Build a list of {@link JobDataOutput} from {@link ValidationResult}'s {@link ValidationDataOutput}
   * @return
   */
  private List<JobDataOutput> buildJobDataOutput(ValidationResult validationResult) {
    return validationResult.getResults().stream()
            .map(ValidationResultElement::getDataOutput)
            .filter( Objects::nonNull )
            .flatMap(List::stream)
            .map( vo -> new JobDataOutput(dataJob.getJobId(), vo))
            .collect(Collectors.toList());
  }

  /**
   * Given 2 collections of {@link ValidationResultElement}, for each element of source merge the issues into
   * the mergeInto collection if it contains a {@link ValidationResultElement} with the same filename. Otherwise,
   * the {@link ValidationResultElement} is added to the mergeInto collection.
   *
   * @param source
   * @param mergeInto
   */
  static void mergeIssuesOnFilename(final Collection<ValidationResultElement> source, final Collection<ValidationResultElement> mergeInto) {
    source.forEach(
            vre -> {
              Optional<ValidationResultElement> currentElement = mergeInto.stream()
                      .filter(re -> vre.getFileName().equals(re.getFileName()))
                      .findFirst();
              if (currentElement.isPresent()) {
                currentElement.get().getIssues().addAll(vre.getIssues());
              } else {
                mergeInto.add(vre);
              }
            }
    );
  }

  /**
   * Emit the provided response to the parent Actor and stop this Actor.
   */
  private void emitResponseAndStop(JobStatusResponse<?> response) {
    context().parent().tell(response, self());
    cleanup();
    getContext().stop(self());
  }

  /**
   * For all {@link JobDataOutput} provided, emit a message.
   * @param dataOutputList
   */
  private void emitDataOutput(List<JobDataOutput> dataOutputList) {
    dataOutputList.forEach( dataOutput -> context().parent().tell(dataOutput, self()));
  }

  private void emitErrorAndStop(DataFile dataFile, ValidationErrorCode errorCode, String errorMessage) {
    JobStatusResponse<?> response = new JobStatusResponse<>(JobStatus.FAILED,
            dataJob.getJobId(), dataJob.getStartTimeStamp(), dataFile.getKey(),
            ValidationResult.onError(dataFile.getSourceFileName(), dataFile.getFileFormat(),
                    dataFile.getReceivedAsMediaType(), errorCode, errorMessage));
    context().parent().tell(response, self());
    cleanup();
    getContext().stop(self());
  }

  /**
   * Deletes the working directory if it exists.
   */
  private void cleanup() {
    if (!preserveTemporaryFiles && workingDir.exists()) {
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
