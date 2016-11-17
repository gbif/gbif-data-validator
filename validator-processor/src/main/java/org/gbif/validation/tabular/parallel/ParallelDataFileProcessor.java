package org.gbif.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

public class ParallelDataFileProcessor implements DataFileProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessor.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final List<Term> termsColumnsMapping;
  private final RecordEvaluator recordEvaluator;
  private final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

  private final Integer fileSplitSize;

  //This instance is shared between all the requests
  private final ActorSystem system;


  /**
   *
   */
  private static class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

    private Set<DataWorkResult> results;
    private int numOfActors;
    private DataFile dataFile;

    ParallelDataFileProcessorMaster(List<RecordMetricsCollector> metricsCollector,
                                    List<ResultsCollector> recordsCollectors, RecordEvaluator recordEvaluator,
                                    List<Term> termsColumnsMapping, Integer fileSplitSize) {
      receive(
        match(DataFile.class, dataFile -> {
          this.dataFile = dataFile;
          processDataFile(fileSplitSize, recordEvaluator);
        })
        .match(DataLine.class, dataLine -> {
          metricsCollector.forEach(c -> c.collect(dataLine.getLine()));
        })
        .match(RecordEvaluationResult.class, recordEvaluationResult -> {
          recordsCollectors.forEach(c -> c.collect(recordEvaluationResult));
        })
        .match(DataWorkResult.class, dataWorkResult -> {
          processResults(dataWorkResult, metricsCollector, recordsCollectors);
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
     *
     * @param result
     * @param metricsCollector
     * @param recordsCollectors
     */
    private void processResults(DataWorkResult result, List<RecordMetricsCollector> metricsCollector,
                                List<ResultsCollector> recordsCollectors) {
      results.add(result);
      if (results.size() == numOfActors) {
        getContext().stop(self());
        getContext().system().shutdown();
        LOG.info("# of lines in the file: {} ", dataFile.getNumOfLines());
        LOG.info("Results: {}", recordsCollectors);
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
                                   ActorSystem system, Integer fileSplitSize) {
    this.termsColumnsMapping = new ArrayList<>(termsColumnsMapping);
    this.recordEvaluator = recordEvaluator;
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
    this.system = system;
    this.fileSplitSize = fileSplitSize;
  }

  @Override
  public RecordsValidationResultElement process(DataFile dataFile) {

    RecordMetricsCollector termsFrequencyCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    ConcurrentValidationCollector resultsCollector = new ConcurrentValidationCollector(ConcurrentValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);

    List<RecordMetricsCollector> metricsCollector = Arrays.asList(termsFrequencyCollector);
    List<ResultsCollector> recordsCollectors = new ArrayList<>(Arrays.asList(resultsCollector));
    interpretedTermsCountCollector.ifPresent(c -> recordsCollectors.add(c));

    // create the master
    ActorRef master = system.actorOf(Props.create(ParallelDataFileProcessorMaster.class, metricsCollector,
            recordsCollectors, recordEvaluator, termsColumnsMapping, fileSplitSize),
            "DataFileProcessor_"+dataFile.getFilePath().getFileName().toString());

    try {
      // start the calculation
      master.tell(dataFile, master);

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
      LOG.info("Processing time for file {}: {} seconds", dataFile.getFilePath(), system.uptime());
    }

    DataFile scopedDataFile = dataFile.isAlternateViewOf().orElse(dataFile);
    return ValidationResultBuilders.RecordsValidationResultElementBuilder
            .of(StringUtils.isNotBlank(dataFile.getSourceFileName()) ? dataFile.getSourceFileName() :
                    scopedDataFile.getSourceFileName(), dataFile.getRowType(),
                    dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1l : 0l))
            .withIssues(resultsCollector.getAggregatedCounts(), resultsCollector.getSamples())
            .withTermsFrequency(termsFrequencyCollector.getTermFrequency())
            .withInterpretedValueCounts(interpretedTermsCountCollector.isPresent() ? interpretedTermsCountCollector.get().getInterpretedCounts() : null)
            .build();
  }

}
