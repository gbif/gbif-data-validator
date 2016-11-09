package org.gbif.validation.tabular.parallel;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final EvaluatorFactory evaluatorFactory;
  private final Integer fileSplitSize;

  //This instance is shared between all the requests
  private final ActorSystem system;

  private final List<Term> termsColumnsMapping;

  private static class ParallelDataFileProcessorMaster extends AbstractLoggingActor {

    private Set<DataWorkResult> results;
    private int numOfActors;
    private DataFile dataFile;
    private List<Term> termsColumnsMapping;

    ParallelDataFileProcessorMaster(List<RecordMetricsCollector> metricsCollector,
                                    List<ResultsCollector> recordsCollectors, EvaluatorFactory evaluatorFactory,
                                    List<Term> termsColumnsMapping, Integer fileSplitSize) {
      receive(
        match(DataFile.class, dataFile -> {
          this.dataFile = dataFile;
          this.termsColumnsMapping = new ArrayList<Term>(termsColumnsMapping);
          processDataFile(fileSplitSize, evaluatorFactory);
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
     * @param occurrenceEvaluatorFactory
     */
    private void processDataFile(Integer fileSplitSize, EvaluatorFactory occurrenceEvaluatorFactory) {
      try {
        int numOfInputRecords = dataFile.getNumOfLines();
        int splitSize = numOfInputRecords > fileSplitSize ?
          (dataFile.getNumOfLines() / fileSplitSize) : 1;
        File outDir = new File(UUID.randomUUID().toString());
        outDir.deleteOnExit();
        String outDirPath = outDir.getAbsolutePath();
        String[] splits = FileBashUtilities.splitFile(dataFile.getFileName(), numOfInputRecords / splitSize, outDirPath);
        numOfActors = splits.length;

        ActorRef workerRouter = getContext().actorOf(
                new RoundRobinPool(numOfActors).props(
                        Props.create(SingleFileReaderActor.class,
                                occurrenceEvaluatorFactory.create(termsColumnsMapping)))
                , "dataFileRouter");
        results =  new HashSet<>(numOfActors);

        for(int i = 0; i < splits.length; i++) {
          DataFile dataInputSplitFile = new DataFile();
          File splitFile = new File(outDirPath, splits[i]);
          splitFile.deleteOnExit();
          dataInputSplitFile.setFileName(splitFile.getAbsolutePath());
          dataInputSplitFile.setSourceFileName(dataInputSplitFile.getSourceFileName());
          dataInputSplitFile.setColumns(dataFile.getColumns());
          dataInputSplitFile.setHasHeaders(dataFile.isHasHeaders() && (i == 0));
          dataInputSplitFile.setFileFormat(dataFile.getFileFormat());
          dataInputSplitFile.setDelimiterChar(dataFile.getDelimiterChar());
          dataInputSplitFile.setFileLineOffset((i * fileSplitSize) + (dataFile.isHasHeaders() ? 1 : 0) );

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

  public ParallelDataFileProcessor(EvaluatorFactory evaluatorFactory, ActorSystem system,
                                   List<Term> termsColumnsMapping, Integer fileSplitSize) {
    this.evaluatorFactory = evaluatorFactory;
    this.system = system;
    this.termsColumnsMapping = new ArrayList<>(termsColumnsMapping);
    this.fileSplitSize = fileSplitSize;
  }

  @Override
  public ValidationResult process(DataFile dataFile) {

    RecordMetricsCollector termsFrequencyCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    ConcurrentValidationCollector resultsCollector = new ConcurrentValidationCollector(ConcurrentValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    //TODO list of terms shall come from config
    InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(
            Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), true);

    List<RecordMetricsCollector> metricsCollector = Arrays.asList(termsFrequencyCollector);
    List<ResultsCollector> recordsCollectors = Arrays.asList(resultsCollector, interpretedTermsCountCollector);

    // create the master
    ActorRef master = system.actorOf(Props.create(ParallelDataFileProcessorMaster.class, metricsCollector,
            recordsCollectors, evaluatorFactory, termsColumnsMapping, fileSplitSize), "DataFileProcessor");
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
      LOG.info("Processing time for file {}: {} seconds", dataFile.getFileName(), system.uptime());
    }
    //FIXME the Status and indexeable should be decided by a another class somewhere
    return ValidationResultBuilders.Builder
            .of(true, dataFile.getSourceFileName(), dataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE)
            .withResourceResult(
                    ValidationResultBuilders.RecordsValidationResultElementBuilder
                            .of(dataFile.getSourceFileName(), dataFile.getRowType(),
                                    dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1l : 0l))
                            .withIssues(resultsCollector.getAggregatedCounts(), resultsCollector.getSamples())
                            .withTermsFrequency(termsFrequencyCollector.getTermFrequency())
                            .withInterpretedValueCounts(interpretedTermsCountCollector.getInterpretedCounts())
                            .build())
            .build();
  }
}
