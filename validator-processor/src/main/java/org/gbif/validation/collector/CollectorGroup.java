package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.EvaluationResultDetails;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResultBuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link CollectorGroup} is used to simplify passing all the collectors around as different entities since they
 * are always used together.
 */
public class CollectorGroup {

  private final RecordMetricsCollector metricsCollector;
  private final RecordEvaluationResultCollector resultsCollector;
  private final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

  private final List<ResultsCollector> recordsCollectors;

  public CollectorGroup(List<Term> termsColumnsMapping, Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
    metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    resultsCollector = new RecordEvaluationResultCollector(RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE, true);
    recordsCollectors = new ArrayList<>();
    recordsCollectors.add(resultsCollector);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
    interpretedTermsCountCollector.ifPresent(c -> recordsCollectors.add(c));
  }

  public RecordMetricsCollector getMetricsCollector() {
    return metricsCollector;
  }

  public List<ResultsCollector> getRecordsCollectors() {
    return recordsCollectors;
  }


  /**
   * Merge all the provided collectors into a single {@link RecordsValidationResultElement}.
   *
   * Not a Thread-Safe operation.
   * @param dataFile
   * @param resultingFileName
   * @param collectors
   * @return
   */
  public static RecordsValidationResultElement mergeAndGetResult(DataFile dataFile, String resultingFileName, List<CollectorGroup> collectors) {

    if (collectors.isEmpty()) {
      return null;
    }

    CollectorGroup baseCollector = collectors.get(0);

    Map<Term, Long> mergedTermFrequency = new HashMap<>(baseCollector.metricsCollector.getTermFrequency());
    Map<EvaluationType, Long> mergedAggregatedCounts = new HashMap<>(baseCollector.resultsCollector.getAggregatedCounts());
    Map<EvaluationType, List<EvaluationResultDetails>> mergedSamples = new HashMap<>(baseCollector.resultsCollector.getSamples());
    Map<Term, Long> mergedInterpretedTermsCount = baseCollector.interpretedTermsCountCollector.get().getInterpretedCounts();

    collectors.stream().skip(1).forEach(coll -> {
              coll.metricsCollector.getTermFrequency().forEach((k, v) -> mergedTermFrequency.merge(k, v, Long::sum));
              coll.resultsCollector.getAggregatedCounts().forEach((k, v) -> mergedAggregatedCounts.merge(k, v, Long::sum));
              coll.interpretedTermsCountCollector.get().getInterpretedCounts().forEach((k, v) -> mergedInterpretedTermsCount.merge(k, v, Long::sum));
              coll.resultsCollector.getSamples().forEach((k, v) -> mergedSamples.merge(k, v, (o, n) -> {
                List<EvaluationResultDetails> a = new ArrayList<>(o);
                a.addAll(n);
                return a;
              }));
            }
    );


    //TODO fix sample size

    return ValidationResultBuilders.RecordsValidationResultElementBuilder
            .of(resultingFileName, dataFile.getRowType(),
                    dataFile.getNumOfLines() - (dataFile.isHasHeaders().orElse(false) ? 1l : 0l))
            .withIssues(mergedAggregatedCounts, mergedSamples)
            .withTermsFrequency(mergedTermFrequency)
            .withInterpretedValueCounts(mergedInterpretedTermsCount)
            .build();
  }

}
