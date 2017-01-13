package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.api.result.ChecklistValidationResult;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.checklists.ChecklistValidator;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.messages.DataJob;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import org.apache.commons.lang3.mutable.MutableInt;

import static org.gbif.validation.api.model.ValidationProfile.GBIF_INDEXING_PROFILE;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Akka actor that acts as the central coordinator of parallel data processing.
 * This class decides if the data input should be split into smaller pieces to be processed by worker actors..
 */
public class DataFileProcessorMaster extends AbstractLoggingActor {

  private final Map<Term, DataFile> rowTypeDataFile;
  private final Map<Term, CollectorGroupProvider> rowTypeCollectorProviders;
  private final Map<Term, List<CollectorGroup>> rowTypeCollectors;
  private final List<ChecklistValidationResult> checklistsResults;
  private final ChecklistValidator checklistValidator;
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;

  //current working directory for the current validation
  private File workingDir;

  /**
   * Simple container class to hold data between initialization and processing phase.
   */
  private static class RecordEvaluationUnit {
    private final List<DataFile> dataFiles;
    private final RecordEvaluator recordEvaluator;
    private final int numOfActors;
    private final CollectorGroupProvider collectorsProvider;

    RecordEvaluationUnit(List<DataFile> dataFiles, RecordEvaluator recordEvaluator, int numOfActors,
                   CollectorGroupProvider collectorsProvider) {
      this.dataFiles = dataFiles;
      this.recordEvaluator = recordEvaluator;
      this.numOfActors = numOfActors;
      this.collectorsProvider = collectorsProvider;
    }
  }

  private static class RowTypeEvaluationUnit {
    private final Term rowType;
    private final RecordCollectionEvaluator resourceRecordIntegrityEvaluator;
    private final CollectorGroupProvider collectorsProvider;

    RowTypeEvaluationUnit(Term rowType,
                            RecordCollectionEvaluator resourceRecordIntegrityEvaluator,
                            CollectorGroupProvider collectorsProvider) {
      this.rowType = rowType;
      this.resourceRecordIntegrityEvaluator = resourceRecordIntegrityEvaluator;
      this.collectorsProvider = collectorsProvider;
    }
  }

  /**
   * Full constructor.
   */
  DataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir,
                          ChecklistValidator  checklistValidator) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectorProviders = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);
    checklistsResults = Collections.synchronizedList(new ArrayList<>());
    this.checklistValidator = checklistValidator;

    receive(
            //this should only be called once
            match(DataJob.class, dataJobMessage -> {
              dataJob = (DataJob<DataFile>)dataJobMessage;
              workingDir = new File(baseWorkingDir, UUID.randomUUID().toString());
              processDataFile(factory, fileSplitSize);
            })
              .match(DataWorkResult.class, this::processResults).build()
    );
  }

  /**
   * Starting point of the entire processing of a {@link DataFile}.
   *
   * @param factory
   * @param fileSplitSize
   * @throws IOException
   */
  private void processDataFile(EvaluatorFactory factory, Integer fileSplitSize) throws IOException {
    DataFile dataFile = dataJob.getJobData();

    if(evaluateResourceStructure(dataFile)){
      List<DataFile> dataFiles = RecordSourceFactory.prepareSource(dataFile);

      init(dataFiles);

      final MutableInt numOfWorkers = new MutableInt(0);
      List<RecordEvaluationUnit> dataFilesToEvaluate = prepareRecordBasedEvaluations(dataFiles, factory, fileSplitSize,
              numOfWorkers);

      List<RowTypeEvaluationUnit> integrity = prepareRowTypeBasedEvaluations(dataFile, dataFiles);
      numOfWorkers.add(integrity.size());

      this.numOfWorkers = numOfWorkers.intValue();

      //now trigger everything
      processRowTypeEvaluationUnit(dataFile, integrity);
      processEvaluationUnit(dataFilesToEvaluate);
    }
  }

  /**
   * Initialize all member variables based on the list of {@link DataFile}.
   * @param dataFiles
   */
  private void init(Iterable<DataFile> dataFiles){
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
   * @param dataFiles all files to evaluate. For DarwinCore it also includes all extensions.
   * @param factory
   * @param fileSplitSize
   * @param numOfWorkers
   * @return
   */
  private List<RecordEvaluationUnit> prepareRecordBasedEvaluations(Iterable<DataFile> dataFiles, EvaluatorFactory factory,
                                                                   Integer fileSplitSize, final MutableInt numOfWorkers) {

    List<RecordEvaluationUnit> dataFilesToEvaluate = new ArrayList<>();

    //prepare everything
    dataFiles.forEach(df -> {
      if (DwcTerm.Taxon == df.getRowType()) {
        numOfWorkers.increment();
        dataFilesToEvaluate.add(new RecordEvaluationUnit(Collections.singletonList(df), null, df.getNumOfLines(), null));
      } else {
        List<Term> columns = Arrays.asList(df.getColumns());
        try {
          List<DataFile> splitDataFile = splitDataFile(df, fileSplitSize);
          numOfWorkers.add(splitDataFile.size());

          dataFilesToEvaluate.add(new RecordEvaluationUnit(splitDataFile,
                  factory.create(df.getRowType(), columns, df.getDefaultValues()),
                  splitDataFile.size(),
                  rowTypeCollectorProviders.get(df.getRowType())));
        } catch (IOException ioEx) {
          log().error("Failed to split data", ioEx);
        }
      }
    });

    log().info("Number of workers required: {}", numOfWorkers);
    return dataFilesToEvaluate;
  }


  /**
   * Prepares the RowTypeEvaluationUnit if required (only on DarwinCore Archive for now).
   * TODO: run uniqueness
   *
   * @param dwcaDataFile
   * @param dataFiles
   *
   * @return all ValidationResultElement or an empty list, never null
   */
  private List<RowTypeEvaluationUnit> prepareRowTypeBasedEvaluations(DataFile dwcaDataFile, List<DataFile> dataFiles) {
    if (FileFormat.DWCA != dwcaDataFile.getFileFormat()) {
      return Collections.emptyList();
    }

    return
            dataFiles.stream()
                    .filter(df -> !df.isCore())
                    .map(df -> new RowTypeEvaluationUnit(
                            df.getRowType(),
                            EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowType()),
                            rowTypeCollectorProviders.get(df.getRowType())))
                    .collect(Collectors.toList());

  }

  /**
   * Evaluate the structure of the resource represented by the provided {@link DataFile}.
   * If an issue is found, the JobStatusResponse will be emitted and this actor will be stopped.
   *
   * @param dataFile
   * @return is the resource structurally valid or not (should the validation continue or not)
   */
  private boolean evaluateResourceStructure(DataFile dataFile) {
    Optional<ValidationResultElement> validationResultElement =
            EvaluatorFactory.createResourceStructureEvaluator(dataFile.getFileFormat())
                    .evaluate(dataFile);

    if (validationResultElement.isPresent()) {
      List<ValidationResultElement> validationResultElementList = new ArrayList<>(1);
      validationResultElementList.add(validationResultElement.get());
      emitResponseAndStop(new JobStatusResponse<>(JobStatus.FINISHED, dataJob.getJobId(),
              new ValidationResult(false, dataFile.getSourceFileName(), dataFile.getFileFormat(),
                      GBIF_INDEXING_PROFILE, validationResultElementList, null, null)));
      return false;
    }
    return true;
  }

  private void processRowTypeEvaluationUnit(DataFile dwcaDataFile, Iterable<RowTypeEvaluationUnit> rowTypeEvaluators) {
    rowTypeEvaluators.forEach(rtEvaluator -> {
      ActorRef actor = createSingleActor(rtEvaluator);
      actor.tell(dwcaDataFile, self());
    });
  }


  /**
   * Creates a Actor/Worker for each evaluation unit.
   */
  private void processEvaluationUnit(Iterable<RecordEvaluationUnit> dataFilesToEvaluate) {
    dataFilesToEvaluate.forEach(evaluationUnit -> {
      ActorRef workerRouter = createWorkerRoutes(evaluationUnit);
      evaluationUnit.dataFiles.forEach(dataFile -> {
        workerRouter.tell(dataFile, self());
      });
    });
  }

  /**
   * Split the provided {@link DataFile} into multiple {@link DataFile} if required.
   * If no split is required the returning list will contain the provided {@link DataFile}.
   * @param dataFile
   * @param fileSplitSize
   * @return
   * @throws IOException
   */
  private List<DataFile> splitDataFile(DataFile dataFile, Integer fileSplitSize) throws IOException {
    List<DataFile> splitDataFiles = new ArrayList<>();
    if(dataFile.getNumOfLines() <= fileSplitSize) {
      splitDataFiles.add(dataFile);
    } else {
      String splitFolder = workingDir.toPath().resolve(dataFile.getRowType().simpleName() + "_split").toAbsolutePath().toString();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), fileSplitSize, splitFolder);

      boolean inputHasHeaders = dataFile.isHasHeaders();
      IntStream.range(0, splits.length)
        .forEach(idx -> splitDataFiles.add(newSplitDataFile(dataFile,
                                                            splitFolder,
                                                            splits[idx],
                                                            inputHasHeaders && (idx == 0),
                                                            Optional.of((idx * fileSplitSize) + (inputHasHeaders ? 1 : 0)))));
    }
    return splitDataFiles;
  }

  /**
   * Get a new {@link DataFile} instance representing a split of the provided {@link DataFile}.
   *
   * @param dataFile
   * @param baseDir
   * @param fileName
   * @param withHeader
   * @param offset
   * @return new {@link DataFile} representing a portion of the provided dataFile.
   */
  private static DataFile newSplitDataFile(DataFile dataFile, String baseDir, String fileName,
                                           boolean withHeader, Optional<Integer> offset){
    //Creates the file to be used
    File splitFile = new File(baseDir, fileName);
    splitFile.deleteOnExit();

    //use dataFile as parent dataFile
    DataFile dataInputSplitFile = DataFile.copyFromParent(dataFile);
    dataInputSplitFile.setHasHeaders(withHeader);
    dataInputSplitFile.setFilePath(Paths.get(splitFile.getAbsolutePath()));
    dataInputSplitFile.setFileLineOffset(offset);
    return dataInputSplitFile;
  }

  /**
   * Creates the worker router using the calculated number of workers.
   */
  private ActorRef createWorkerRoutes(RecordEvaluationUnit evaluationUnit) {
    String actorName =  "dataFileRouter_" + UUID.randomUUID();
    if (evaluationUnit.dataFiles.size() == 1 && DwcTerm.Taxon == evaluationUnit.dataFiles.get(0).getRowType()) {
      return getContext().actorOf(Props.create(ChecklistsValidatorActor.class, checklistValidator),actorName);
    }
    return getContext().actorOf(
            new RoundRobinPool(10).props(Props.create(DataFileRecordsActor.class,
                                                                              evaluationUnit.recordEvaluator,
                                                                              evaluationUnit.collectorsProvider)),
            actorName);
  }

  /**
   * Creates an Actor for the provided {@link RowTypeEvaluationUnit}.
   */
  private ActorRef createSingleActor(RowTypeEvaluationUnit rowTypeEvaluationUnit) {
    String actorName =  "RowTypeEvaluationUnitActor_" + UUID.randomUUID();
      return getContext().actorOf(Props.create(DataFileRowTypeActor.class,
              rowTypeEvaluationUnit.rowType,
              rowTypeEvaluationUnit.resourceRecordIntegrityEvaluator,
              rowTypeEvaluationUnit.collectorsProvider), actorName);
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

    //FIXME we should only use one collector
    if (DwcTerm.Taxon != result.getRowType()) {
      rowTypeCollectors.compute(result.getRowType(), (key, val) -> {
        val.add(result.getCollectors());
        return val;
      });
    } else {
      checklistsResults.add(result.getChecklistValidationResult());
    }
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
    return new ValidationResult(true, dataJob.getJobData().getSourceFileName(), dataJob.getJobData().getFileFormat(),
            GBIF_INDEXING_PROFILE, validationResultElements, null, checklistsResults);
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
