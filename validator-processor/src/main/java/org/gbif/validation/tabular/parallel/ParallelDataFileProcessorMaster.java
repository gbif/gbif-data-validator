package org.gbif.validation.tabular.parallel;

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
  private int numOfActors;
  private DataJob<DataFile> dataJob;
  private DataFile dataFile;
  private ParallelResultCollector collector;

  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize) {
    receive(
      match(DataJob.class, dataJobMessage -> {
        dataJob = dataJobMessage;
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
          processResults(dataWorkResult, collector);
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

      int numOfInputRecords = dataFile.getNumOfLines();
      int splitSize = numOfInputRecords > fileSplitSize ?
        (dataFile.getNumOfLines() / fileSplitSize) : 1;
      File outDir = new File(UUID.randomUUID().toString());
      outDir.deleteOnExit();
      String outDirPath = outDir.getAbsolutePath();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), numOfInputRecords / splitSize, outDirPath);
      numOfActors = splits.length;

      ActorRef workerRouter = getContext().actorOf(
        new RoundRobinPool(numOfActors).props(
          Props.create(SingleFileReaderActor.class, recordEvaluator))
        , "dataFileRouter");
      results =  new HashSet<>(numOfActors);

      for(int i = 0; i < splits.length; i++) {
        DataFile dataInputSplitFile = new DataFile(dataFile);
        File splitFile = new File(outDirPath, splits[i]);
        splitFile.deleteOnExit();
        dataInputSplitFile.setFilePath(Paths.get(splitFile.getAbsolutePath()));
        dataInputSplitFile.setSourceFileName(dataInputSplitFile.getSourceFileName());
        dataInputSplitFile.setColumns(dataFile.getColumns());
        dataInputSplitFile.setHasHeaders(Optional.of(dataFile.isHasHeaders().orElse(false) && (i == 0)));
        dataInputSplitFile.setFileFormat(dataFile.getFileFormat());
        dataInputSplitFile.setDelimiterChar(dataFile.getDelimiterChar());
        dataInputSplitFile.setFileLineOffset(Optional.of((i * fileSplitSize) +
                                                         (dataFile.isHasHeaders().orElse(false) ? 1 : 0)));

        workerRouter.tell(dataInputSplitFile, self());
      }
    } catch (IOException ex) {
      LOG.error("Error processing file", ex);
    }
  }

  /**
   * Called when a single worker finished its work.
   */
  private void processResults(DataWorkResult result, ParallelResultCollector collector) {
    results.add(result);
    if (results.size() == numOfActors) {
      ValidationResultBuilders.Builder blrd = ValidationResultBuilders.Builder.of(true, dataFile.getSourceFileName(), dataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE);
      blrd.withResourceResult(collector.toResult(dataFile, dataFile.getSourceFileName()));
      context().parent().tell(new JobStatusResponse(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(), blrd.build()), self());
      getContext().stop(self());
      LOG.info("# of lines in the file: {} ", dataFile.getNumOfLines());
      LOG.info("Results: {}", collector.recordsCollectors);
    }
  }

}
