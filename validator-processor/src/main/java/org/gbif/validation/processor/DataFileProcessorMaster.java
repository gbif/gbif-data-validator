package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.FileFormat;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * This class decides if the data input should be split into smaller pieces to be processed by worker actors..
 */
public class DataFileProcessorMaster extends AbstractLoggingActor {

  private static final int MAX_WORKER = Runtime.getRuntime().availableProcessors() * 50;

  private final Map<Term, DataFile> rowTypeDataFile;
  private final Map<Term, CollectorGroupProvider> rowTypeCollectorProviders;
  private final Map<Term, List<CollectorGroup>> rowTypeCollectors;
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
    private final CollectorGroupProvider collectorsProvider;

    RecordEvaluationUnit(List<DataFile> dataFiles, RecordEvaluator recordEvaluator,
                   CollectorGroupProvider collectorsProvider) {
      this.dataFiles = dataFiles;
      this.recordEvaluator = recordEvaluator;
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
  DataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectorProviders = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);

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
      List<DataFile> dataFiles = DataFileFactory.prepareDataFile(dataFile);

      init(dataFiles);

      //numOfWorkers is used to know how many responses we are expecting
      final MutableInt numOfWorkers = new MutableInt(0);
      List<RecordEvaluationUnit> dataFilesToEvaluate = prepareRecordBasedEvaluations(dataFiles, factory, fileSplitSize,
              numOfWorkers);

      List<RowTypeEvaluationUnit> rowTypeBasedEvaluations = prepareRowTypeBasedEvaluations(dataFile, dataFiles, factory);
      numOfWorkers.add(rowTypeBasedEvaluations.size());

      this.numOfWorkers = numOfWorkers.intValue();

      //now trigger everything
      processRowTypeEvaluationUnit(dataFile, rowTypeBasedEvaluations);
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
   *
   * @param dataFiles     all files to evaluate. For DarwinCore it also includes all extensions.
   * @param factory
   * @param fileSplitSize
   * @param numOfWorkers
   *
   * @return
   */
  private List<RecordEvaluationUnit> prepareRecordBasedEvaluations(Iterable<DataFile> dataFiles, EvaluatorFactory factory,
                                                                   Integer fileSplitSize, final MutableInt numOfWorkers) {

    List<RecordEvaluationUnit> dataFilesToEvaluate = new ArrayList<>();

    //prepare everything
    dataFiles.forEach(df -> {
      List<Term> columns = Arrays.asList(df.getColumns());
      try {
        List<DataFile> splitDataFile = DataFileSplitter.splitDataFile(df, fileSplitSize, workingDir.toPath());
        numOfWorkers.add(splitDataFile.size());

        dataFilesToEvaluate.add(new RecordEvaluationUnit(splitDataFile,
                factory.create(df.getRowType(), columns, df.getDefaultValues()),
                rowTypeCollectorProviders.get(df.getRowType())));
      } catch (IOException ioEx) {
        log().error("Failed to split data", ioEx);
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
  private List<RowTypeEvaluationUnit> prepareRowTypeBasedEvaluations(DataFile dwcaDataFile, List<DataFile> dataFiles,
                                                                     EvaluatorFactory factory) {
    if (FileFormat.DWCA != dwcaDataFile.getFileFormat()) {
      return Collections.emptyList();
    }

    List<RowTypeEvaluationUnit> rowTypeEvaluationUnits =
            dataFiles.stream()
            .filter(df -> df.getType() == DwcFileType.EXTENSION)
            .map(df -> new RowTypeEvaluationUnit(
                    df.getRowType(),
                    EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowType()),
                    rowTypeCollectorProviders.get(df.getRowType())))
            .collect(Collectors.toList());

    dataFiles.stream().filter( df -> DwcTerm.Taxon.equals(df.getRowType()))
            .findFirst()
            .ifPresent(df -> rowTypeEvaluationUnits.add(
                    new RowTypeEvaluationUnit(
                            DwcTerm.Taxon,
                            factory.createChecklistEvaluator(),
                            rowTypeCollectorProviders.get(DwcTerm.Taxon)
            )));

    return rowTypeEvaluationUnits;

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
                      GBIF_INDEXING_PROFILE, validationResultElementList, null)));
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
   * Creates the worker router using the calculated number of workers.
   */
  private ActorRef createWorkerRoutes(RecordEvaluationUnit evaluationUnit) {
    String actorName =  "dataFileRouter_" + UUID.randomUUID();
    return getContext().actorOf(
            new RoundRobinPool(Math.min(evaluationUnit.dataFiles.size(), MAX_WORKER)).props(Props.create(DataFileRecordsActor.class,
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
