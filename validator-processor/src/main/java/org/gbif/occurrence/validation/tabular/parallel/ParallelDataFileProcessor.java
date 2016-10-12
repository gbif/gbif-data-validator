package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.api.model.DataFileValidationResult;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;
import org.gbif.occurrence.validation.evaluator.OccurrenceEvaluatorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.japi.pf.ReceiveBuilder.match;

public class ParallelDataFileProcessor implements DataFileProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessor.class);

  private static final int FILE_SPLIT_SIZE = 10000;

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final String apiUrl;


  private static class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

    private Set<DataWorkResult> results;
    private int numOfActors;
    private DataFile dataFile;


    ParallelDataFileProcessorMaster(ResultsCollector collector, OccurrenceEvaluatorFactory occurrenceEvaluatorFactory) {
      receive(
        match(DataFile.class, dataFile  -> {
          this.dataFile = dataFile;
          processDataFile(occurrenceEvaluatorFactory);
        })
        .match(RecordEvaluationResult.class, collector::accumulate)
        .match(DataWorkResult.class, dataWorkResult -> {
          processResults(dataWorkResult, collector);
        }).build()
      );
    }

    private void processDataFile(OccurrenceEvaluatorFactory occurrenceEvaluatorFactory) {
      try {
        int numOfInputRecords = dataFile.getNumOfLines();
        int splitSize = numOfInputRecords > FILE_SPLIT_SIZE ?
          (dataFile.getNumOfLines() / FILE_SPLIT_SIZE) : 1;
        File outDir = new File(UUID.randomUUID().toString());
        outDir.deleteOnExit();
        String outDirPath = outDir.getAbsolutePath();
        String[] splits = FileBashUtilities.splitFile(dataFile.getFileName(), numOfInputRecords / splitSize, outDirPath);
        numOfActors = splits.length;
        ActorRef workerRouter = getContext().actorOf(new RoundRobinPool(numOfActors).props(Props.create(SingleFileReaderActor.class, occurrenceEvaluatorFactory.create(new String[]{""}))), "dataFileRouter");
        results =  new HashSet<>(numOfActors);

        for(int i = 0; i < splits.length; i++) {
          DataFile dataInputSplitFile = new DataFile();
          File splitFile = new File(outDirPath, splits[i]);
          splitFile.deleteOnExit();
          dataInputSplitFile.setFileName(splitFile.getAbsolutePath());
          dataInputSplitFile.setColumns(dataFile.getColumns());
          dataInputSplitFile.setHasHeaders(dataFile.isHasHeaders() && (i == 0));
          dataInputSplitFile.setFileLineOffset((i * FILE_SPLIT_SIZE) + (dataFile.isHasHeaders() ? 1 : 0) );

          workerRouter.tell(dataInputSplitFile, self());
        }
      } catch (IOException ex) {
        LOG.error("Error processing file",ex);
      }
    }


    private void processResults(DataWorkResult result, ResultsCollector collector) {
      results.add(result);
      if (results.size() == numOfActors) {
        getContext().stop(self());
        getContext().system().shutdown();
        LOG.info("# of lines in the file: {} ", dataFile.getNumOfLines());
        LOG.info("Results: {}", collector);
      }
    }

  }

  public ParallelDataFileProcessor(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  @Override
  public DataFileValidationResult process(DataFile dataFile) {
    ConcurrentValidationCollector validationCollector = new ConcurrentValidationCollector(ResultsCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    // Create an Akka system
    ActorSystem system = ActorSystem.create("DataFileProcessorSystem");

    // create the master
    ActorRef master = system.actorOf(Props.create(ParallelDataFileProcessorMaster.class, validationCollector,
                                                  new OccurrenceEvaluatorFactory(apiUrl)), "DataFileProcessor");
    try {
      // start the calculation
      master.tell(dataFile,master);
      while (!master.isTerminated()) {
        try {
          Thread.sleep(SLEEP_TIME_BEFORE_TERMINATION);
        } catch (InterruptedException ie) {
          LOG.error("Thread interrupted", ie);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      system.shutdown();
      LOG.info("Processing time for file {}: {} seconds", dataFile.getFileName(), system.uptime());
    }
    //FIXME the Status and indexeable should be decided by a another class somewhere
    return new DataFileValidationResult(
            validationCollector.getAggregatedCounts().isEmpty() ? DataFileValidationResult.Status.OK : DataFileValidationResult.Status.FAILED,
            true, validationCollector.getAggregatedCounts(), validationCollector.getSamples());
  }
}