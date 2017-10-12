package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.api.result.ValidationDataOutput;
import org.gbif.validation.api.result.ValidationResultDetails;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link CollectorGroup} is used to simplify passing all the collectors around as different entities since they
 * are always used together.
 */
public class CollectorGroup {

  private static final Logger LOG = LoggerFactory.getLogger(CollectorGroup.class);

  private final RecordMetricsCollector metricsCollector;
  private final RecordEvaluationResultCollector resultsCollector;
  private final InterpretedTermsCountCollector interpretedTermsCountCollector;

  private final List<ResultsCollector> recordsCollectors;

  CollectorGroup(List<Term> termsColumnsMapping, InterpretedTermsCountCollector interpretedTermsCountCollector) {
    metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    resultsCollector = new RecordEvaluationResultCollector(RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE, true);
    recordsCollectors = new ArrayList<>();
    recordsCollectors.add(resultsCollector);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;

    if(interpretedTermsCountCollector != null) {
      recordsCollectors.add(interpretedTermsCountCollector);
    }
  }

  /**
   * Call collect on all metrics collector(s)
   * @param record
   */
  public void collectMetrics(List<String> record) {
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
            newHashMapInit(Optional.ofNullable(baseCollector.interpretedTermsCountCollector).map(
                    InterpretedTermsCountCollector::getInterpretedCounts));

    collectors.stream().skip(1).forEach(coll -> {
              coll.metricsCollector.getTermFrequency().forEach((k, v) -> mergedTermFrequency.merge(k, v, Long::sum));
              coll.resultsCollector.getAggregatedCounts().forEach((k, v) -> mergedAggregatedCounts.merge(k, v, Long::sum));
              if(coll.interpretedTermsCountCollector != null) {
                coll.interpretedTermsCountCollector.getInterpretedCounts()
                        .forEach((k, v) -> mergedInterpretedTermsCount.merge(k, v, Long::sum));
              }
              coll.resultsCollector.getSamples().forEach((k, v) -> mergedSamples.merge(k, v, (o, n) -> {
                List<ValidationResultDetails> a = new ArrayList<>(o);
                a.addAll(n);
                return a;
              }));
            }
    );

    // Fix sample size after merging
    Map<EvaluationType, List<ValidationResultDetails>> resampledMergedSamples = mergedSamples.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> resample(e.getValue(), RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE)));

    // transform the term frequency into an ordered list of key/value pairs
    List<Map.Entry<Term, Integer>> termFrequency = Arrays.stream(dataFile.getColumns())
            .filter(Objects::nonNull)
            .map(t -> new AbstractMap.SimpleImmutableEntry<>(t, mergedTermFrequency.getOrDefault(t, -1L).intValue()))
            .collect(Collectors.toList());
    LOG.warn("dataFile.getColumns() ->" + Arrays.toString(dataFile.getColumns()));

    //
    Map<Long, List<String>> verbatimRecordSample = getFullVerbatimRecordSample(dataFile.getColumns(), collectors);
    List<ValidationDataOutput> dataOutput =
            Collections.singletonList(ValidationDataOutput.verbatimRecordSample(dataFile.getColumns(),
                    verbatimRecordSample));

    return new ValidationResultElement(resultingFileName,
            dataFile.getNumOfLines().longValue(),
            dataFile.getDwcFileType(),
            dataFile.getRowTypeKey().getRowType(),
            dataFile.getRecordIdentifier().map(TermIndex::getTerm).orElse(null),
            mergedAggregatedCounts, resampledMergedSamples,
            termFrequency,
            mergedInterpretedTermsCount, dataOutput);
  }


  /**
   * Get a {@link Map} of verbatim record by record number with the list of verbatim values in the same order
   * as the provided headers.
   *
   * @param headers
   * @param collectors
   *
   * @return
   */
  public static Map<Long, List<String>> getFullVerbatimRecordSample(final Term[] headers, List<CollectorGroup> collectors) {
    Map<Long, List<String>> orderedVerbatimRecords = new TreeMap<>();
    collectors.stream().forEach(coll -> {
      coll.resultsCollector.getFullRecordSamples().entrySet()
              .forEach(es -> orderedVerbatimRecords.put(es.getKey(), toOrderedVerbatimValues(headers, es.getValue())));
    });
    return orderedVerbatimRecords;
  }

  private static List<String> toOrderedVerbatimValues(Term[] headers, Map<Term, String> verbatimData) {
    List<String> orderedValues = new ArrayList<>(headers.length);
    Arrays.stream(headers)
            .forEach(t -> orderedValues.add(verbatimData.get(t)));
    return orderedValues;
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
