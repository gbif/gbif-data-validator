package org.gbif.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.CollectorFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.japi.pf.ReceiveBuilder.match;

public class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessorMaster.class);

  private final Map<Term, ParallelResultCollector> rowTypeCollector;
  private final Map<Term, DataFile> rowTypeDataFile;
  private final AtomicInteger workerCompleted;

  private int numOfWorkers;

  private DataJob<DataFile> dataJob;
  private File workingDir;

  private static class EvaluationUnit {
    private List<DataFile> dataFiles;
    private RecordEvaluator recordEvaluator;
    public EvaluationUnit(List<DataFile> dataFiles, RecordEvaluator recordEvaluator) {
     this.dataFiles = dataFiles;
      this.recordEvaluator = recordEvaluator;
    }
  }

  /**
   * Calculates the number of records that will be processed by each data worker.
   */
  private static int recordsPerSplit(int fileSizeInLines, int fileSplitSize){
    return fileSizeInLines / Math.max(fileSizeInLines/fileSplitSize, 1);
  }


  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, final String baseWorkingDir) {

    rowTypeCollector = new ConcurrentHashMap<>();
    rowTypeDataFile = new ConcurrentHashMap<>();
    workerCompleted = new AtomicInteger(0);

    receive(
      //this should only be called once
      match(DataJob.class, dataJobMessage -> {
        dataJob = dataJobMessage;
        workingDir = new File(baseWorkingDir,UUID.randomUUID().toString());
        processDataFile((DataFile)dataJobMessage.getJobData(),factory,fileSplitSize);
      })
        .match(DataLine.class, dataLine -> {
          rowTypeCollector.get(dataLine.getRowType()).metricsCollector.collect(dataLine.getLine());
        })
        .match(RecordEvaluationResult.class, recordEvaluationResult -> {
          rowTypeCollector.get(recordEvaluationResult.getRowType()).recordsCollectors.forEach(c -> c.collect(recordEvaluationResult));
        })
        .match(DataWorkResult.class, dataWorkResult -> {
          processResults(dataWorkResult);
        }).build()
    );
  }

  private void processDataFile(DataFile dataFile, EvaluatorFactory factory, Integer fileSplitSize) throws IOException {
    //TODO check why is necessary
    dataFile.setHasHeaders(Optional.of(true));
    List<DataFile> dataFiles = RecordSourceFactory.prepareSource(dataFile);
    List<EvaluationUnit> dataFilesToEvaluate = splitDataFile(dataFiles, factory, fileSplitSize);
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
      rowTypeCollector.put(df.getRowType(), new ParallelResultCollector(Arrays.asList(df.getColumns()),
                                                                        CollectorFactory.createInterpretedTermsCountCollector(df.getRowType(), true)));
      rowTypeDataFile.put(df.getRowType(), df);
      try {
        List<DataFile> splitDataFile = splitDataFile(df, fileSplitSize);
        numOfWorkers += splitDataFile.size();
        dataFilesToEvaluate.add(new EvaluationUnit(splitDataFile,
                                                   factory.create(Arrays.asList(df.getColumns()), df.getRowType())));
      } catch (IOException ioEx) {
        LOG.error("Failed to split data", ioEx);
      }

    });
    return dataFilesToEvaluate;
  }

  /**
   * Creates a Actor/Worker for each evaluation unit.
   */
  private void processDataFile(List<EvaluationUnit> dataFilesToEvaluate) {
    dataFilesToEvaluate.forEach(dfte -> {
      ActorRef workerRouter = createWorkerRoutes(dfte.recordEvaluator);
      dfte.dataFiles.forEach(df -> workerRouter.tell(df, self()));
    });
  }

  /**
   * Split the provided {@link DataFile} into multiple {@link DataFile} if required.
   * @param dataFile
   * @param fileSplitSize
   * @return
   * @throws IOException
   */
  private List<DataFile> splitDataFile(DataFile dataFile, Integer fileSplitSize) throws IOException {
    int recordsPerSplit = recordsPerSplit(dataFile.getNumOfLines(), fileSplitSize);
    String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), recordsPerSplit, workingDir.getAbsolutePath());
    List<DataFile> splitDataFiles = new ArrayList<>();
    boolean inputHasHeaders = dataFile.isHasHeaders().orElse(false);

    IntStream.range(0, splits.length)
            .forEach(idx ->
                    splitDataFiles.add(newSplitDataFile(dataFile, workingDir.getAbsolutePath(), splits[idx],
                            Optional.of(inputHasHeaders && (idx == 0)),
                            Optional.of((idx * recordsPerSplit) + (inputHasHeaders ? 1 : 0))))
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
  private ActorRef createWorkerRoutes(RecordEvaluator recordEvaluator) {
    return getContext().actorOf(
      new RoundRobinPool(numOfWorkers).props(Props.create(SingleFileReaderActor.class, recordEvaluator)),
      "dataFileRouter");
  }



  /**
   * Called when a single worker finished its work.
   * This can represent en entire file or a part of a file
   */
  private void processResults(DataWorkResult result) {
    int numberOfWorkersCompleted = workerCompleted.incrementAndGet();
    if (numberOfWorkersCompleted == numOfWorkers) {
      //prepare validationResultBuilder
      ValidationResultBuilders.Builder validationResultBuilder =
              ValidationResultBuilders.Builder.of(true, dataJob.getJobData().getSourceFileName(),
              dataJob.getJobData().getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE);

      rowTypeCollector.forEach((rowType, collector) -> validationResultBuilder.withResourceResult(collector
              .toResult(rowTypeDataFile.get(rowType), rowTypeDataFile.get(rowType).getSourceFileName())));
      context().parent().tell(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(), validationResultBuilder.build()), self());
      FileUtils.deleteDirectoryRecursively(workingDir);
      getContext().stop(self());
    }
  }

}
