package org.gbif.validation.tabular.parallel;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.CollectorFactory;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.DataJob;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.japi.pf.ReceiveBuilder.match;

public class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessorMaster.class);

  private Set<DataWorkResult> results;
  private int numOfWorkers;
  private DataJob<DataFile> dataJob;
  private DataFile dataFile;
  private ParallelResultCollector collector;
  private File workingDir;

  /**
   * Calculates the number of records that will be processed by each data worker.
   */
  private static int recordsPerSplit(int fileSizeInLines, int fileSplitSize){
    return fileSizeInLines / Math.max(fileSizeInLines/fileSplitSize, 1);
  }

  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize, final String baseWorkingDir) {
    receive(
      match(DataJob.class, dataJobMessage -> {
        dataJob = dataJobMessage;
        workingDir = new File(baseWorkingDir,UUID.randomUUID().toString());
        dataFile =  (DataFile)dataJobMessage.getJobData();
        dataFile.setHasHeaders(Optional.of(true));
        dataFile = RecordSourceFactory.prepareSource(dataFile).get(0);
        RecordEvaluator recordEvaluator = factory.create(Arrays.asList(dataFile.getColumns()),dataFile.getRowType());
        dataJob = (DataJob<DataFile>) dataJobMessage;
        collector = new ParallelResultCollector(Arrays.asList(dataFile.getColumns()),
                                                CollectorFactory.createInterpretedTermsCountCollector(dataFile.getRowType(), true));
        processDataFile(fileSplitSize, recordEvaluator);
      })
        .match(DataLine.class, dataLine -> {
          collector.metricsCollector.collect(dataLine.getLine());
        })
        .match(RecordEvaluationResult.class, recordEvaluationResult -> {
          collector.recordsCollectors.forEach(c -> c.collect(recordEvaluationResult));
        })
        .match(DataWorkResult.class, dataWorkResult -> {
          processResults(dataWorkResult);
        }).build()
    );
  }

  /**
   *
   * @param fileSplitSize
   * @param recordEvaluator
   */
  private void processDataFile(Integer fileSplitSize, RecordEvaluator recordEvaluator) {
    try {
      int recordsPerSplit = recordsPerSplit(dataFile.getNumOfLines(),fileSplitSize);
      String outDirPath = workingDir.getAbsolutePath();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), recordsPerSplit, outDirPath);
      numOfWorkers = splits.length;
      results =  new HashSet<>(numOfWorkers);

      ActorRef workerRouter = createWorkerRoutes(recordEvaluator);
      boolean inputHasHeaders = dataFile.isHasHeaders().orElse(false);
      for(int i = 0; i < splits.length; i++) {
        DataFile dataInputSplitFile = buildDataSplitFile(outDirPath, splits[i],
                                                         Optional.of(inputHasHeaders && (i == 0)),
                                                         Optional.of((i * recordsPerSplit) + (inputHasHeaders? 1 : 0)));
        workerRouter.tell(dataInputSplitFile, self());
      }
    } catch (IOException ex) {
      LOG.error("Error processing file", ex);
    }
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
   * Builds a DataFile split that will be assigned to a data worker.
   */
  private DataFile buildDataSplitFile(String baseDir, String fileName, Optional<Boolean> withHeader,
                                      Optional<Integer> offset){
    //Creates the file to be used
    File splitFile = new File(baseDir, fileName);
    splitFile.deleteOnExit();
    DataFile dataInputSplitFile = new DataFile(dataFile);
    dataInputSplitFile.setFilePath(Paths.get(splitFile.getAbsolutePath()));
    dataInputSplitFile.setSourceFileName(dataInputSplitFile.getSourceFileName());
    dataInputSplitFile.setColumns(dataFile.getColumns());
    dataInputSplitFile.setHasHeaders(withHeader);
    dataInputSplitFile.setFileFormat(dataFile.getFileFormat());
    dataInputSplitFile.setDelimiterChar(dataFile.getDelimiterChar());
    dataInputSplitFile.setFileLineOffset(offset);
    return dataInputSplitFile;
  }

  /**
   * Called when a single worker finished its work.
   */
  private void processResults(DataWorkResult result) {
    results.add(result);
    if (results.size() == numOfWorkers) {
      ValidationResultBuilders.Builder blrd = ValidationResultBuilders.Builder.of(true, dataFile.getSourceFileName(),
                                                                                  dataFile.getFileFormat(),
                                                                                  ValidationProfile.GBIF_INDEXING_PROFILE);
      blrd.withResourceResult(collector.toResult(dataFile, dataFile.getSourceFileName()));
      context().parent().tell(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(), blrd.build()), self());
      FileUtils.deleteDirectoryRecursively(workingDir);
      getContext().stop(self());
      LOG.info("# of lines in the file: {} ", dataFile.getNumOfLines());
      LOG.info("Results: {}", collector.recordsCollectors);
    }
  }

}
