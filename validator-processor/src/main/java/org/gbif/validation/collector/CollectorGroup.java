package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.EvaluationResultDetails;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.RecordEvaluationResultCollector.SampledEvaluationResultDetails;

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

  public CollectorGroup(List<Term> termsColumnsMapping, Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
    metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    resultsCollector = new RecordEvaluationResultCollector(RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE, true);
    recordsCollectors = new ArrayList<>();
    recordsCollectors.add(resultsCollector);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
    interpretedTermsCountCollector.ifPresent(recordsCollectors::add);
  }

  public RecordMetricsCollector getMetricsCollector() {
    return metricsCollector;
  }

  public List<ResultsCollector> getRecordsCollectors() {
    return recordsCollectors;
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
  public static ValidationResultElement mergeAndGetResult(DataFile dataFile, String resultingFileName,
                                                          List<CollectorGroup> collectors) {

    if (collectors.isEmpty()) {
      return null;
    }

    CollectorGroup baseCollector = collectors.get(0);

    Map<Term, Long> mergedTermFrequency = CollectorUtils.newHashMapInit(
            baseCollector.metricsCollector.getTermFrequency());
    Map<EvaluationType, Long> mergedAggregatedCounts = CollectorUtils.
            newEvaluationTypeEnumMap(baseCollector.resultsCollector.getAggregatedCounts());
    Map<EvaluationType, List<SampledEvaluationResultDetails>> mergedSamples = CollectorUtils.
            newEvaluationTypeEnumMap(baseCollector.resultsCollector.getSamples());

    Map<Term, Long> mergedInterpretedTermsCount = CollectorUtils.
            newHashMapInit(baseCollector.interpretedTermsCountCollector.map(
                    InterpretedTermsCountCollector::getInterpretedCounts));

    collectors.stream().skip(1).forEach(coll -> {
              coll.metricsCollector.getTermFrequency().forEach((k, v) -> mergedTermFrequency.merge(k, v, Long::sum));
              coll.resultsCollector.getAggregatedCounts().forEach((k, v) -> mergedAggregatedCounts.merge(k, v, Long::sum));
              coll.interpretedTermsCountCollector.ifPresent(itcc -> itcc.getInterpretedCounts().forEach((k, v) -> mergedInterpretedTermsCount.merge(k, v, Long::sum)));
              coll.resultsCollector.getSamples().forEach((k, v) -> mergedSamples.merge(k, v, (o, n) -> {
                List<SampledEvaluationResultDetails> a = new ArrayList<>(o);
                a.addAll(n);
                return a;
              }));
            }
    );

    //Fix sample size after merging
    Map<EvaluationType, List<EvaluationResultDetails>> resampledMergedSamples = mergedSamples.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> resample(e.getValue(), RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE)));

    return new ValidationResultElement(resultingFileName,
            dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1l : 0l),
            dataFile.getRowType(),
            mergedAggregatedCounts, resampledMergedSamples,
            mergedTermFrequency,
            mergedInterpretedTermsCount);
  }


  /**
   * Take a list of {@link EvaluationResultDetails} and make sure the sample size is not greater than
   * maxSample.
   *
   * @param resultDetails
   * @param maxSample
   *
   * @return
   */
  private static List<EvaluationResultDetails> resample(List<SampledEvaluationResultDetails> resultDetails,
                                                                 int maxSample) {
    return resultDetails.stream()
            .sorted((lberd1, lberd2) -> Long.compare(lberd1.getLineNumber(), lberd2.getLineNumber()))
            .limit(maxSample)
            .map(SampledEvaluationResultDetails::getDetails)
            .collect(Collectors.toList());
  }

}
