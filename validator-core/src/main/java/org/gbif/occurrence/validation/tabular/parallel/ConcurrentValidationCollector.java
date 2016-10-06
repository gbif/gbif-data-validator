package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.EvaluationResult;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.model.RecordInterpretationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Collects results of data validations produced from a multi-threaded processing.
 *
 * The accumulate method is thread-safe, getAggregatedCounts and getSamples are NOT thread-safe.
 */
public class ConcurrentValidationCollector implements ResultsCollector {

  private final int maxNumberOfSample;

  private final Map<EvaluationType, ConcurrentHashMap<EvaluationDetailType, LongAdder>> issueCounter;
  private final Map<EvaluationType, ConcurrentHashMap<EvaluationDetailType, ConcurrentLinkedQueue<EvaluationResultDetails>>> issueSampling;

  private final LongAdder recordCount;

  /**
   *
   * @param maxNumberOfSample maximum number of sample to take per {@link EvaluationDetailType}
   */
  public ConcurrentValidationCollector(Integer maxNumberOfSample) {

    this.maxNumberOfSample = maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE;

    issueCounter = new ConcurrentHashMap<>(EvaluationType.values().length);
    issueSampling = new ConcurrentHashMap<>(EvaluationType.values().length);

    recordCount = new LongAdder();
  }

  @Override
  public void accumulate(EvaluationResult result) {

    issueCounter.putIfAbsent(result.getEvaluationType(), new ConcurrentHashMap<>());
    issueSampling.putIfAbsent(result.getEvaluationType(), new ConcurrentHashMap<>());

    collectData(result.getDetails(), issueCounter.get(result.getEvaluationType()),
            issueSampling.get(result.getEvaluationType()));

    switch (result.getEvaluationType()) {
      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
        break;
      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretationResult)result);
        break;
    }
  }

  private void accumulate(RecordInterpretationResult result) {
    recordCount.increment();
  }

  private void accumulate(RecordStructureEvaluationResult result) {

  }

  /**
   * @return a copy of the inter aggregated counts.
   */
  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, Long>> getAggregatedCounts() {

    Map<EvaluationType, Map<EvaluationDetailType, Long>> copy = new EnumMap<>(EvaluationType.class);

    issueCounter.entrySet().forEach( rec -> {
              copy.put(rec.getKey(), new HashMap<>());
              rec.getValue().entrySet().forEach(entry ->
                        copy.get(rec.getKey()).put(entry.getKey(), entry.getValue().longValue()));
            }
    );
    return copy;
  }

  /**
   *
   * @return a copy of the internal evaluation samples.
   */
  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> getSamples() {
    Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> copy = new EnumMap<>(EvaluationType.class);

    issueSampling.entrySet().forEach( rec -> {
              copy.put(rec.getKey(), new HashMap<>());
              rec.getValue().entrySet().forEach(entry ->
                      copy.get(rec.getKey()).put(entry.getKey(),
                              new ArrayList<>(entry.getValue())));
            }
    );
    return copy;
  }


  /**
   * Function used to collect data from a list of {@link EvaluationResultDetails}.
   */
  private void collectData(Iterable<EvaluationResultDetails> details, Map<EvaluationDetailType, LongAdder> counter,
                           Map<EvaluationDetailType, ConcurrentLinkedQueue<EvaluationResultDetails>> samples) {
    details.forEach(
            detail -> {
              counter.computeIfAbsent(detail.getEvaluationDetailType(), k -> new LongAdder()).increment();

              samples.putIfAbsent(detail.getEvaluationDetailType(), new ConcurrentLinkedQueue<>());
              samples.compute(detail.getEvaluationDetailType(), (type, queue) -> {
                        if(queue.size() < maxNumberOfSample){
                          samples.get(type).add(detail);
                        }
                        return queue;
              });
            }
    );
  }

  @Override
  public String toString() {
    return "ConcurrentValidationCollector{" +
           "recordCount=" + recordCount +
           '}';
  }
}
