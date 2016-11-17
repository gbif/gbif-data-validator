package org.gbif.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.DataFileProcessorAsync;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationJobResponse;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.japi.pf.ReceiveBuilder.match;

public class ParallelDataFileProcessor implements DataFileProcessor, DataFileProcessorAsync {


  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessor.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final List<Term> termsColumnsMapping;
  private final RecordEvaluator recordEvaluator;

  private final Integer fileSplitSize;

  //This instance is shared between all the requests
  private final ActorSystem system;

  private final long jobId;

  private final ParallelResultCollector collector;

  private static class ParallelResultCollector {

    final RecordMetricsCollector metricsCollector;
    final ConcurrentValidationCollector resultsCollector;
    final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

    final List<ResultsCollector> recordsCollectors;

    public ParallelResultCollector(List<Term> termsColumnsMapping, Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
      metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
      resultsCollector = new ConcurrentValidationCollector(ConcurrentValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
      recordsCollectors = new ArrayList<>();
      recordsCollectors.add(resultsCollector);
      this.interpretedTermsCountCollector = interpretedTermsCountCollector;
      interpretedTermsCountCollector.ifPresent(c -> recordsCollectors.add(c));
    }

    public RecordsValidationResultElement toResult(DataFile dataFile, String resultingFileName){
      return ValidationResultBuilders.RecordsValidationResultElementBuilder
        .of(resultingFileName, dataFile.getRowType(),
            dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1l : 0l))
        .withIssues(resultsCollector.getAggregatedCounts(), resultsCollector.getSamples())
        .withTermsFrequency(metricsCollector.getTermFrequency())
        .withInterpretedValueCounts(interpretedTermsCountCollector.isPresent() ? interpretedTermsCountCollector.get().getInterpretedCounts() : null)
        .build();
    }
  }

  /**
   *
   */
  private static class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

    private Set<DataWorkResult> results;
    private int numOfActors;
    private DataFile dataFile;
    private ParallelResultCollector collector;

    ParallelDataFileProcessorMaster(ParallelResultCollector collector, RecordEvaluator recordEvaluator,
                                    List<Term> termsColumnsMapping, Integer fileSplitSize) {
      receive(
        match(DataFile.class, dataFile -> {
          this.dataFile = dataFile;
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
          dataInputSplitFile.setHasHeaders(dataFile.isHasHeaders() && (i == 0));
          dataInputSplitFile.setFileFormat(dataFile.getFileFormat());
          dataInputSplitFile.setDelimiterChar(dataFile.getDelimiterChar());
          dataInputSplitFile.setFileLineOffset(Optional.of((i * fileSplitSize) + (dataFile.isHasHeaders() ? 1 : 0)) );

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
        getContext().stop(self());
        LOG.info("# of lines in the file: {} ", dataFile.getNumOfLines());
        LOG.info("Results: {}", collector.recordsCollectors);
      }
    }

  }

  /**
   *
   * @param termsColumnsMapping
   * @param recordEvaluator
   * @param interpretedTermsCountCollector
   * @param system
   * @param fileSplitSize
   */
  public ParallelDataFileProcessor(List<Term> termsColumnsMapping, RecordEvaluator recordEvaluator,
                                   Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector,
                                   ActorSystem system, Integer fileSplitSize, long jobId) {

    this.termsColumnsMapping = new ArrayList<>(termsColumnsMapping);
    collector = new ParallelResultCollector(termsColumnsMapping, interpretedTermsCountCollector);
    this.recordEvaluator = recordEvaluator;
    this.system = system;
    this.fileSplitSize = fileSplitSize;
    this.jobId = jobId;
  }

  @Override
  public RecordsValidationResultElement process(DataFile dataFile) {
    ActorRef master = createMasterActor();
    master.tell(dataFile, master);
    waitForActor(master, SLEEP_TIME_BEFORE_TERMINATION);
    LOG.info("File processed {}", dataFile.getFilePath());
    DataFile scopedDataFile = dataFile.isAlternateViewOf().orElse(dataFile);
    return collector.toResult(dataFile, StringUtils.isNotBlank(dataFile.getSourceFileName()) ? dataFile.getSourceFileName() :
      scopedDataFile.getSourceFileName());
  }

  /**
   * Waits 'millis' for the actorRef to finish
   */
  private static void waitForActor(ActorRef actorRef, long millis){
      while (!actorRef.isTerminated()) {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException ie) {
          LOG.error("Thread interrupted", ie);
        }
      }
  }

  /**
   * Process a file asynchronously.
   */
  @Override
  public ValidationJobResponse processAsync(DataFile dataFile) {
    // create the master
    ActorRef master = createMasterActor();
    master.tell(dataFile, master);
    return new ValidationJobResponse(ValidationJobResponse.JobStatus.ACCEPTED, jobId);
  }

  /**
   * Creates the master actor.
   */
  private ActorRef createMasterActor() {
    // create the master
    return system.actorOf(Props.create(ParallelDataFileProcessorMaster.class, collector,
                                       recordEvaluator, termsColumnsMapping, fileSplitSize),
                          "DataFileProcessor_" + jobId);

  }

}
