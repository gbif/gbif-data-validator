package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.api.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.api.model.EvaluationType;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import java.util.ArrayList;
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

  private final Map<EvaluationType, LongAdder> issueCounter;
  private final Map<EvaluationType, ConcurrentLinkedQueue<EvaluationResultDetails>> issueSampling;

  /**
   *
   * @param maxNumberOfSample maximum number of sample to take per {@link EvaluationDetailType}
   */
  public ConcurrentValidationCollector(Integer maxNumberOfSample) {

    this.maxNumberOfSample = maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE;

    issueCounter = new ConcurrentHashMap<>(EvaluationType.values().length);
    issueSampling = new ConcurrentHashMap<>(EvaluationType.values().length);
  }

  @Override
  public void accumulate(RecordEvaluationResult result) {

    if(result.getDetails() == null){
      return;
    }

    result.getDetails().forEach(
            detail -> {
              issueCounter.computeIfAbsent(detail.getEvaluationDetailType(), k -> new LongAdder()).increment();

              issueSampling.putIfAbsent(detail.getEvaluationDetailType(), new ConcurrentLinkedQueue<>());
              issueSampling.compute(detail.getEvaluationDetailType(), (type, queue) -> {
                if (queue.size() < maxNumberOfSample) {
                  issueSampling.get(type).add(detail);
                }
                return queue;
              });
            }
    );
  }

  /**
   * @return a copy of the inter aggregated counts.
   */
  @Override
  public Map<EvaluationType, Long> getAggregatedCounts() {

    Map<EvaluationType, Long> copy = new HashMap<>();
    issueCounter.entrySet().forEach(rec -> copy.put(rec.getKey(), rec.getValue().longValue()));
    return copy;
  }

  /**
   *
   * @return a copy of the internal evaluation samples.
   */
  @Override
  public Map<EvaluationType, List<EvaluationResultDetails>> getSamples() {
    Map<EvaluationType, List<EvaluationResultDetails>> copy = new HashMap<>();

    issueSampling.entrySet().forEach( rec -> copy.put(rec.getKey(),
                              new ArrayList<>(rec.getValue()))
    );
    return copy;
  }

}
