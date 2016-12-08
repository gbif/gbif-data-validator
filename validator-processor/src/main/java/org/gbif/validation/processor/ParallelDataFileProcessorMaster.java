package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResultBuilders;
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
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;

import static akka.japi.pf.ReceiveBuilder.match;

public class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

  private final Map<Term, DataFile> rowTypeDataFile;
  private final Map<Term, List<CollectorGroup>> rowTypeCollectors;
  private final ChecklistValidator checklistValidator;
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;

  //current working directory for the current validation
  private File workingDir;

  private static class EvaluationUnit {
    private final List<DataFile> dataFiles;
    private final RecordEvaluator recordEvaluator;
    private final int numOfActors;
    private final CollectorGroupProvider collectorsProvider;
    EvaluationUnit(List<DataFile> dataFiles, RecordEvaluator recordEvaluator, int numOfActors, CollectorGroupProvider collectorsProvider) {
      this.dataFiles = dataFiles;
      this.recordEvaluator = recordEvaluator;
      this.numOfActors = numOfActors;
      this.collectorsProvider = collectorsProvider;
    }
  }

  /**
   * Calculates the number of records that will be processed by each data worker.
   */
  private static int recordsPerSplit(int fileSizeInLines, int fileSplitSize){
    return fileSizeInLines / Math.max(fileSizeInLines/fileSplitSize, 1);
  }


  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, String baseWorkingDir,
                                  ChecklistValidator  checklistValidator) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);
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

  private void processDataFile(EvaluatorFactory factory, Integer fileSplitSize) throws IOException {
    //TODO check why is necessary
    DataFile dataFile = dataJob.getJobData();
    dataFile.setHasHeaders(Optional.of(true));
    List<DataFile> dataFiles = RecordSourceFactory.prepareSource(dataFile);
    List<EvaluationUnit> dataFilesToEvaluate = prepareDataFile(dataFiles, factory, fileSplitSize);
    //now trigger everything
    processDataFile(dataFilesToEvaluate);
  }

  /**
   * Calculates the required splits to process all the datafiles. The variable numOfWorkers is changed with the number
   * of actors that will be used to process the input file.
   */
  private List<EvaluationUnit> splitDataFile(List<DataFile> dataFiles, EvaluatorFactory factory, Integer fileSplitSize) {
    List<EvaluationUnit> dataFilesToEvaluate = new ArrayList<>();

    numOfWorkers = 0;
    //prepare everything
    dataFiles.forEach(df -> {
      if (DwcTerm.Taxon == df.getRowType()) {
        numOfWorkers += 1;
        dataFilesToEvaluate.add(new EvaluationUnit(Collections.singletonList(df),null,df.getNumOfLines(),null));
      } else {
        rowTypeCollectors.putIfAbsent(df.getRowType(), new ArrayList<>());
        rowTypeDataFile.put(df.getRowType(), df);
        try {
          List<DataFile> splitDataFile = splitDataFile(df, fileSplitSize);
          numOfWorkers += splitDataFile.size();
          List<Term> columns = Arrays.asList(df.getColumns());
          dataFilesToEvaluate.add(new EvaluationUnit(splitDataFile,
                                                     factory.create(columns, df.getRowType()),
                                                     splitDataFile.size(),
                                                     new CollectorGroupProvider(df.getRowType(), columns)));
        } catch (IOException ioEx) {
          log().error("Failed to split data", ioEx);
        }
      }
    });

    log().info("Number of workers required: {}", numOfWorkers);
    return dataFilesToEvaluate;
  }

  /**
   * Creates a Actor/Worker for each evaluation unit.
   */
  private void processDataFile(Iterable<EvaluationUnit> dataFilesToEvaluate) {
    dataFilesToEvaluate.forEach(evaluationUnit -> {
      ActorRef workerRouter = createWorkerRoutes(evaluationUnit);
      evaluationUnit.dataFiles.forEach(dataFile -> workerRouter.tell(dataFile, self()));
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
      return splitDataFiles;
    }

    String splitFolder = workingDir.toPath().resolve(dataFile.getRowType().simpleName() + "_split").toAbsolutePath().toString();
    String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), fileSplitSize, splitFolder);

    boolean inputHasHeaders = dataFile.isHasHeaders().orElse(false);
    IntStream.range(0, splits.length)
            .forEach(idx ->
                    splitDataFiles.add(newSplitDataFile(dataFile, splitFolder, splits[idx],
                            Optional.of(inputHasHeaders && (idx == 0)),
                            Optional.of((idx * fileSplitSize) + (inputHasHeaders ? 1 : 0))))
            );
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
  private static DataFile newSplitDataFile(DataFile dataFile, String baseDir, String fileName, Optional<Boolean> withHeader,
                                      Optional<Integer> offset){
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
  private ActorRef createWorkerRoutes(EvaluationUnit evaluationUnit) {
    String actorName =  "dataFileRouter_" + UUID.randomUUID();
    if (evaluationUnit.dataFiles.size() == 1 && DwcTerm.Taxon == evaluationUnit.dataFiles.get(0).getRowType()) {
      return getContext().actorOf(Props.create(ChecklistsValidatorActor.class, checklistValidator),actorName);
    }
    return getContext().actorOf(
            new RoundRobinPool(evaluationUnit.numOfActors).props(Props.create(SingleFileReaderActor.class,
                                                                              evaluationUnit.recordEvaluator,
                                                                              evaluationUnit.collectorsProvider)),
            actorName);
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
    if (DwcTerm.Taxon != result.getDataFile().getRowType()) {
      rowTypeCollectors.compute(result.getDataFile().getRowType(), (key, val) -> {
        val.add(result.getCollectors());
        return val;
      });
    }
    log().info("Got {} worker response(s)", numberOfWorkersCompleted);
    if (numberOfWorkersCompleted == numOfWorkers) {

      //prepare validationResultBuilder
      ValidationResultBuilders.Builder validationResultBuilder =
              ValidationResultBuilders.Builder.of(true, dataJob.getJobData().getSourceFileName(),
              dataJob.getJobData().getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE);
      if (DwcTerm.Taxon == result.getDataFile().getRowType()) {
        validationResultBuilder.withChecklistValidationResult(result.getChecklistValidationResult());
      } else {
        rowTypeCollectors.forEach((rowType, collectorList) -> validationResultBuilder.withResourceResult(CollectorGroup.mergeAndGetResult(
          rowTypeDataFile.get(rowType),
          rowTypeDataFile.get(rowType).getSourceFileName(),
          collectorList)));
        FileUtils.deleteDirectoryRecursively(workingDir);
      }
      context().parent().tell(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(), validationResultBuilder.build()), self());
      getContext().stop(self());
    }
  }

}
