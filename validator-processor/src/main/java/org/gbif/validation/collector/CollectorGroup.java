package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.api.result.ValidationResultDetails;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The {@link CollectorGroup} is used to simplify passing all the collectors around as different entities since they
 * are always used together.
 */
public class CollectorGroup {

  private final RecordMetricsCollector metricsCollector;
  private final RecordEvaluationResultCollector resultsCollector;
  private final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

  private final List<ResultsCollector> recordsCollectors;

  CollectorGroup(List<Term> termsColumnsMapping, Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
    metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    resultsCollector = new RecordEvaluationResultCollector(RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE, true);
    recordsCollectors = new ArrayList<>();
    recordsCollectors.add(resultsCollector);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
    interpretedTermsCountCollector.ifPresent(recordsCollectors::add);
  }

  /**
   * Call collect on all metrics collector(s)
   * @param record
   */
  public void collectMetrics(String[] record) {
    metricsCollector.collect(record);
  }

  /**
   * Call collect() on all record collector(s).
   * @param result
   */
  public void collectResult(RecordEvaluationResult result) {
    recordsCollectors.forEach(c -> c.collect(result));
  }

  /**
   * Merge all the provided collectors into a single {@link ValidationResultElement}.
   *
   * Not a Thread-Safe operation.
   * @param dataFile
   * @param resultingFileName
   * @param collectors
   * @return
   */
  public static ValidationResultElement mergeAndGetResult(TabularDataFile dataFile, String resultingFileName,
                                                          List<CollectorGroup> collectors) {

    if (collectors.isEmpty()) {
      return null;
    }

    CollectorGroup baseCollector = collectors.get(0);

    Map<Term, Long> mergedTermFrequency = CollectorUtils.newHashMapInit(
            baseCollector.metricsCollector.getTermFrequency());
    Map<EvaluationType, Long> mergedAggregatedCounts = CollectorUtils.
            newEvaluationTypeEnumMap(baseCollector.resultsCollector.getAggregatedCounts());
    Map<EvaluationType, List<ValidationResultDetails>> mergedSamples = CollectorUtils.
            newEvaluationTypeEnumMap(baseCollector.resultsCollector.getSamples());

    Map<Term, Long> mergedInterpretedTermsCount = CollectorUtils.
            newHashMapInit(baseCollector.interpretedTermsCountCollector.map(
                    InterpretedTermsCountCollector::getInterpretedCounts));

    collectors.stream().skip(1).forEach(coll -> {
              coll.metricsCollector.getTermFrequency().forEach((k, v) -> mergedTermFrequency.merge(k, v, Long::sum));
              coll.resultsCollector.getAggregatedCounts().forEach((k, v) -> mergedAggregatedCounts.merge(k, v, Long::sum));
              coll.interpretedTermsCountCollector.ifPresent(itcc -> itcc.getInterpretedCounts().forEach((k, v) -> mergedInterpretedTermsCount.merge(k, v, Long::sum)));
              coll.resultsCollector.getSamples().forEach((k, v) -> mergedSamples.merge(k, v, (o, n) -> {
                List<ValidationResultDetails> a = new ArrayList<>(o);
                a.addAll(n);
                return a;
              }));
            }
    );

    //Fix sample size after merging
    Map<EvaluationType, List<ValidationResultDetails>> resampledMergedSamples = mergedSamples.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> resample(e.getValue(), RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE)));

    return new ValidationResultElement(resultingFileName,
            dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1l : 0l),
            dataFile.getType(),
            dataFile.getRowType(),
            mergedAggregatedCounts, resampledMergedSamples,
            mergedTermFrequency,
            mergedInterpretedTermsCount);
  }


  /**
   * Take a list of {@link RecordEvaluationResultDetails} and make sure the sample size is not greater than
   * maxSample.
   *
   * @param resultDetails
   * @param maxSample
   *
   * @return
   */
  private static List<ValidationResultDetails> resample(List<ValidationResultDetails> resultDetails,
                                                        int maxSample) {
    return resultDetails.stream()
            .sorted((lberd1, lberd2) -> Long.compare(lberd1.getLineNumber() == null ? Long.MAX_VALUE : lberd1.getLineNumber(),
                    lberd2.getLineNumber() == null ? Long.MAX_VALUE : lberd2.getLineNumber()))
            .limit(maxSample)
            .collect(Collectors.toList());
  }

}
