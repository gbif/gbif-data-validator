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
import org.gbif.validation.evaluator.IndexableRules;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;

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

  private final AtomicInteger numOfWorkers;
  private final AtomicInteger workerCompleted;
  private final AtomicBoolean initCompleted;

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
    numOfWorkers = new AtomicInteger(0);
    initCompleted = new AtomicBoolean(false);
    validationResultElements = new ConcurrentLinkedQueue<>();
    this.preserveTemporaryFiles = preserveTemporaryFiles;

    receive(
            //this should only be called once
            match(DataJob.class, dataJobMessage -> {
              dataJob = (DataJob<DataFile>) dataJobMessage;
              workingDir = new File(baseWorkingDir, UUID.randomUUID().toString());
              workingDir.mkdir();
              processDataFile(factory, fileSplitSize);
            })
                    .match(DataWorkResult.class, this::processRecordBasedResults)
                    .match(MetadataWorkResult.class, this::processMetadataBasedResults)
                    .match(FinishedInit.class, this::onInitCompleted).build()
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
    DwcDataFileSupplier transformer = () -> DataFileFactory.prepareDataFile(dataFile, workingDir.toPath());

   // final MutableInt numOfWorkers = new MutableInt(0);
    EvaluationChain.Builder evaluationChainBuilder =
            EvaluationChain.Builder.using(dataFile, transformer, factory, workingDir.toPath())
                    .evaluateMetadataContent()
                    .evaluateCoreUniqueness()
                    .evaluateReferentialIntegrity()
                    .evaluateRecords(df -> handleSplit(df, fileSplitSize))
                    .evaluateChecklist();
    EvaluationChain evaluationChain = evaluationChainBuilder.build();

    ResourceConstitutionEvaluationChain.ResourceConstitutionResult resourceConstitutionResults = evaluationChain.runResourceConstitutionEvaluation();
    ValidationResultElement.mergeOnFilename(resourceConstitutionResults.getResults(), validationResultElements);

    if(resourceConstitutionResults.isEvaluationStopped()) {
      emitResponseAndStop(buildJobStatusResponse(false, JobStatus.FINISHED, dataFile, new ArrayList<>(validationResultElements)));
      return;
    }

    //notify the parent with partial results
    context().parent()
            .tell(buildJobStatusResponse(null, JobStatus.RUNNING, dataFile, new ArrayList<>(validationResultElements)), self());
    DwcDataFile dwcDataFile = resourceConstitutionResults.getTransformedDataFile();

    init(dwcDataFile.getTabularDataFiles());

    //numOfWorkers.add(evaluationChain.getNumberOfRowTypeEvaluationUnits());
    //numOfWorkers.add(evaluationChain.getNumberOfDwcDataFileEvaluationUnits());

   // this.numOfWorkers = numOfWorkers.intValue();

    //log().info("Expected {} worker response(s)", numOfWorkers);
   // log().info(evaluationChain.toString());

    //now trigger everything
    startAllActors(evaluationChain, numOfWorkers);

    //this.numOfWorkers = numOfWorkers.intValue();
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
   * Handle splits to process all the {@link DataFile} (if required).
   *
   * @param tabularDataFile
   * @param fileSplitSize
   *
   * @return
   */
  private List<TabularDataFile> handleSplit(final TabularDataFile tabularDataFile, final Integer fileSplitSize) throws IOException {
    List<TabularDataFile> splitDataFile;
    try {
      splitDataFile = DataFileSplitter.splitDataFile(tabularDataFile, fileSplitSize, workingDir.toPath());
    } catch (IOException ioEx) {
      log().error("Failed to split data", ioEx);
      throw ioEx;
    }
    return splitDataFile;
  }

  /**
   * Triggers processing of all dwcDataFileEvaluator, recordCollectionEvaluator and RecordEvaluator from the chain.
   *
   * @param evaluationChain
   */
  private void startAllActors(final EvaluationChain evaluationChain, final AtomicInteger numOfWorkers) {
    DwcDataFileEvaluatorRunner dwcDataFileEvaluatorRunner = (dwcDataFile, dwcDataFileEvaluator) -> {
      numOfWorkers.incrementAndGet();
      ActorRef actor = createSingleActor(dwcDataFileEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runDwcDataFileEvaluation(dwcDataFileEvaluatorRunner);

    RecordCollectionEvaluatorRunner runner = (dwcDataFile, rowTypeKey, recordCollectionEvaluator) -> {
      numOfWorkers.incrementAndGet();
      ActorRef actor = createSingleActor(rowTypeKey, recordCollectionEvaluator);
      actor.tell(dwcDataFile, self());
    };
    evaluationChain.runRecordCollectionEvaluation(runner);

    RecordEvaluatorRunner recordEvaluatorRunner = (dataFiles, rowTypeKey, recordEvaluator) -> {
      log().info("RecordEvaluatorRunner got {} dataFiles", dataFiles.size());
      numOfWorkers.addAndGet(dataFiles.size());
      ActorRef workerRouter = createWorkerRoutes(Math.min(dataFiles.size(), MAX_WORKER),
              rowTypeKey, recordEvaluator);
      dataFiles.forEach(dataFile -> workerRouter.tell(dataFile, self()));
    };

    try {
      evaluationChain.runRecordEvaluation(recordEvaluatorRunner);
    } catch (IOException ioEx) {
      emitErrorAndStop(evaluationChain.getDataFile(), ValidationErrorCode.IO_ERROR, ioEx.getMessage());
      return;
    }
    log().info("Expected {} worker response(s)", numOfWorkers.get());
    this.self().tell(FinishedInit.INSTANCE, self());
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
   * Creates an Actor for the provided {@link DwcDataFileEvaluator}.
   */
  private ActorRef createSingleActor(DwcDataFileEvaluator metadataEvaluator) {
    String actorName =  "MetadataEvaluatorActor_" + UUID.randomUUID();
    return getContext().actorOf(Props.create(MetadataContentActor.class, metadataEvaluator), actorName);
  }

  /**
   * Creates an Actor for the provided {@link RowTypeKey} and {@link RecordCollectionEvaluator}.
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
              ValidationResultElement.mergeOnFilename(ver, validationResultElements));
    }
    incrementWorkerCompleted();
  }

  /**
   * Once a response is received, increment counter and check for completion.
   * In theory, this method is only called by actors so thread safety should be included
   */
  private void incrementWorkerCompleted() {
    int numberOfWorkersCompleted = workerCompleted.incrementAndGet();
    log().info("Got {} worker response(s)", numberOfWorkersCompleted);


    checkCompleteness(numberOfWorkersCompleted);
  }

  /**
   * In theory, this method is only called by actors so thread safety should be included
   */
  private void onInitCompleted(FinishedInit ignore) {
    initCompleted.set(true);
    checkCompleteness(workerCompleted.get());
  }

  private void checkCompleteness(int numberOfWorkersCompleted) {
    // in theory, this method is only called by actors so thread safety should be included
    if (numberOfWorkersCompleted == numOfWorkers.get() && initCompleted.get()) {
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
    ValidationResultElement.mergeOnFilename(validationResultElements, resultElements);

    return new ValidationResult(IndexableRules.isIndexable(resultElements), dataJob.getJobData().getSourceFileName(),
            dataJob.getJobData().getFileFormat(),
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

}
