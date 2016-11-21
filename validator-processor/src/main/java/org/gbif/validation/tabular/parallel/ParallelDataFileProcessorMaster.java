package org.gbif.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorFactory;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.DataJob;
import org.gbif.validation.jobserver.DataJobResult;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
  private ParallelResultCollector collector;

  ParallelDataFileProcessorMaster(EvaluatorFactory factory, Integer fileSplitSize) {
    receive(
      match(DataJob.class, dataJobMessage -> {
        dataJob = dataJobMessage;
        RecordEvaluator recordEvaluator = factory.create(Arrays.asList(dataJob.getJobData().getColumns()),dataJob.getJobData().getRowType());
        dataJob = (DataJob<DataFile>) dataJobMessage;
        collector = new ParallelResultCollector(Arrays.asList(dataJob.getJobData().getColumns()),
                                                CollectorFactory.createInterpretedTermsCountCollector(dataJob.getJobData().getRowType(), true));
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

      int numOfInputRecords = dataJob.getJobData().getNumOfLines();
      int splitSize = numOfInputRecords > fileSplitSize ?
        (dataJob.getJobData().getNumOfLines() / fileSplitSize) : 1;
      File outDir = new File(UUID.randomUUID().toString());
      outDir.deleteOnExit();
      String outDirPath = outDir.getAbsolutePath();
      String[] splits = FileBashUtilities.splitFile(dataJob.getJobData().getFilePath().toString(), numOfInputRecords / splitSize, outDirPath);
      numOfActors = splits.length;

      ActorRef workerRouter = getContext().actorOf(
        new RoundRobinPool(numOfActors).props(
          Props.create(SingleFileReaderActor.class, recordEvaluator))
        , "dataFileRouter");
      results =  new HashSet<>(numOfActors);

      for(int i = 0; i < splits.length; i++) {
        DataFile dataInputSplitFile = new DataFile(dataJob.getJobData());
        File splitFile = new File(outDirPath, splits[i]);
        splitFile.deleteOnExit();
        dataInputSplitFile.setFilePath(Paths.get(splitFile.getAbsolutePath()));
        dataInputSplitFile.setSourceFileName(dataInputSplitFile.getSourceFileName());
        dataInputSplitFile.setColumns(dataJob.getJobData().getColumns());
        dataInputSplitFile.setHasHeaders(Optional.of(dataJob.getJobData().isHasHeaders().orElse(false) && (i == 0)));
        dataInputSplitFile.setFileFormat(dataJob.getJobData().getFileFormat());
        dataInputSplitFile.setDelimiterChar(dataJob.getJobData().getDelimiterChar());
        dataInputSplitFile.setFileLineOffset(Optional.of((i * fileSplitSize) +
                                                         (dataJob.getJobData().isHasHeaders().orElse(false) ? 1 : 0)));

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
      ValidationResultBuilders.Builder blrd = ValidationResultBuilders.Builder.of(true, dataJob.getJobData().getSourceFileName(), dataJob.getJobData().getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE);
      blrd.withResourceResult(collector.toResult(dataJob.getJobData(), dataJob.getJobData().getSourceFileName()));
      sender().tell(new DataJobResult<ValidationResult>(dataJob, blrd.build()), self());
      getContext().stop(self());
      LOG.info("# of lines in the file: {} ", dataJob.getJobData().getNumOfLines());
      LOG.info("Results: {}", collector.recordsCollectors);
    }
  }

}
