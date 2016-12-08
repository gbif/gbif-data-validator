package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
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
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;

  //current working directory for the current validation
  private File workingDir;

  /**
   * Simple container class to hold data between initialization and processing phase.
   */
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
   *
   * @param factory
   * @param fileSplitSize
   * @param baseWorkingDir base working directory under which a new folder will be created for the validation (using a random UUID)
   */
  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, final String baseWorkingDir) {

    rowTypeDataFile = new ConcurrentHashMap<>();
    rowTypeCollectors = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);

    receive(
            //this should only be called once
            match(DataJob.class, dataJobMessage -> {
              dataJob = dataJobMessage;
              workingDir = new File(baseWorkingDir, UUID.randomUUID().toString());
              processDataFile((DataFile) dataJobMessage.getJobData(), factory, fileSplitSize);
            })
                    .match(DataWorkResult.class, dataWorkResult -> {
                      processResults(dataWorkResult);
                    }).build()
    );
  }

  private void processDataFile(DataFile dataFile, EvaluatorFactory factory, Integer fileSplitSize) throws IOException {

    //Validate the structure of the resource
    Optional<ValidationResultElement> validationResultElement =
            EvaluatorFactory.createResourceStructureEvaluator(dataFile.getFileFormat())
                    .evaluate(dataFile.getFilePath(), dataFile.getSourceFileName());
    if (validationResultElement.isPresent()) {
      emitResponseAndStop(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(),
              ValidationResultBuilders.Builder.of(false, dataFile.getSourceFileName(),
                      dataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE)
                      .withResourceResult(validationResultElement.get()).build()));
      return;
    }

    List<DataFile> dataFiles = RecordSourceFactory.prepareSource(dataFile);
    //TODO ensure we have a rowType
    List<EvaluationUnit> dataFilesToEvaluate = prepareDataFile(dataFiles, factory, fileSplitSize);
    //now trigger everything
    processDataFile(dataFilesToEvaluate);
  }

  /**
   * Calculates the required splits to process all the datafiles. The variable numOfWorkers is changed with the number
   * of actors that will be used to process the input file.
   */
  private List<EvaluationUnit> prepareDataFile(List<DataFile> dataFiles, EvaluatorFactory factory, Integer fileSplitSize) {
    List<EvaluationUnit> dataFilesToEvaluate = new ArrayList<>();

    numOfWorkers = 0;
    //prepare everything
    dataFiles.forEach(df -> {
      rowTypeCollectors.putIfAbsent(df.getRowType(), new ArrayList<>());
      rowTypeDataFile.put(df.getRowType(), df);
      try {
        List<DataFile> splitDataFile = splitDataFile(df, fileSplitSize);
        numOfWorkers += splitDataFile.size();
        List<Term> columns = Arrays.asList(df.getColumns());
        dataFilesToEvaluate.add(new EvaluationUnit(splitDataFile,
                factory.create(columns, df.getRowType()),
                splitDataFile.size(), new CollectorGroupProvider(df.getRowType(), columns)));
      } catch (IOException ioEx) {
        log().error("Failed to split data", ioEx);
      }
    });

    log().info("Number of workers required: {}", numOfWorkers);
    return dataFilesToEvaluate;
  }

  /**
   * Creates a Actor/Worker for each evaluation unit.
   */
  private void processDataFile(List<EvaluationUnit> dataFilesToEvaluate) {
    dataFilesToEvaluate.forEach(dfte -> {
      ActorRef workerRouter = createWorkerRoutes(dfte.recordEvaluator, dfte.collectorsProvider, dfte.numOfActors);
      dfte.dataFiles.forEach(df -> workerRouter.tell(df, self()));
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
  private ActorRef createWorkerRoutes(RecordEvaluator recordEvaluator, CollectorGroupProvider collectorsProvider, int numOfActors) {
    return getContext().actorOf(
            new RoundRobinPool(numOfActors).props(Props.create(SingleFileReaderActor.class, recordEvaluator, collectorsProvider)),
            "dataFileRouter_" + UUID.randomUUID().toString());
  }

  /**
   * Called when a single worker finished its work.
   * This can represent en entire file or a part of a file
   */
  private void processResults(DataWorkResult result) {

    //FIXME
    if(result.getResult() == DataWorkResult.Result.FAILED) {
      log().error("DataWorkResult = FAILED");
    }

    int numberOfWorkersCompleted = workerCompleted.incrementAndGet();
    rowTypeCollectors.compute(result.getDataFile().getRowType(), (key, val) -> {
      val.add(result.getCollectors());
      return val;
    });
    log().info("Got {} worker response(s)", numberOfWorkersCompleted);
    if (numberOfWorkersCompleted == numOfWorkers) {

      //prepare validationResultBuilder
      ValidationResultBuilders.Builder validationResultBuilder =
              ValidationResultBuilders.Builder.of(true, dataJob.getJobData().getSourceFileName(),
              dataJob.getJobData().getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE);

      rowTypeCollectors.forEach((rowType, collectorList) -> validationResultBuilder.withResourceResult(
              CollectorGroup.mergeAndGetResult(rowTypeDataFile.get(rowType), rowTypeDataFile.get(rowType).getSourceFileName(),
              collectorList)));
      emitResponseAndStop(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(), validationResultBuilder.build()));
    }
  }

  /**
   * Emit the provided response to the parent Actor and stop this Actor.
   *
   * @param response
   */
  private void emitResponseAndStop(JobStatusResponse response) {
    context().parent().tell(response, self());
    FileUtils.deleteDirectoryRecursively(workingDir);
    getContext().stop(self());
  }

}
