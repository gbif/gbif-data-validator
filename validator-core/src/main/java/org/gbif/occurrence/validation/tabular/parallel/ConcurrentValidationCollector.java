package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.EvaluationResult;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Collects results of data validations produced from a multi-threaded processing.
 */
@ThreadSafe
public class ConcurrentValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private static final int MAX_SAMPLE_SIZE = 10;

  private final ConcurrentHashMap<EvaluationType, ConcurrentHashMap<EvaluationDetailType, LongAdder>> issueCounter;
  private ConcurrentHashMap<EvaluationType, ConcurrentHashMap<EvaluationDetailType, ConcurrentLinkedQueue<EvaluationResultDetails>>> issueSampling;

  private final LongAdder recordCount;

  public ConcurrentValidationCollector() {
    issueCounter = new ConcurrentHashMap<>(EvaluationType.values().length);
    issueSampling = new ConcurrentHashMap<>(EvaluationType.values().length);

    recordCount = new LongAdder();
  }

  public void accumulate(EvaluationResult result) {

    issueCounter.putIfAbsent(result.getEvaluationType(), new ConcurrentHashMap<>());
    issueSampling.putIfAbsent(result.getEvaluationType(), new ConcurrentHashMap<>());

    collectData(result.getDetails(), issueCounter.get(result.getEvaluationType()),
            issueSampling.get(result.getEvaluationType()));

    switch (result.getEvaluationType()) {
      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
        break;
      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretionBasedEvaluationResult)result);
        break;
    }
  }

  /**
   * @return a copy of the inter aggregated counts.
   */
  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, Long>> getAggregatedCounts() {

    Map<EvaluationType, Map<EvaluationDetailType, Long>> copy = new HashMap<>();

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
    Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> copy = new HashMap<>();

    issueSampling.entrySet().forEach( rec -> {
              copy.put(rec.getKey(), new HashMap<>());
              rec.getValue().entrySet().forEach(entry ->
                      copy.get(rec.getKey()).put(entry.getKey(),
                              new ArrayList<>(entry.getValue())));
            }
    );

    return copy;
  }


  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount.increment();
  }

  public void accumulate(RecordStructureEvaluationResult result) {

  }

  private void collectData(List<EvaluationResultDetails> details,
                                ConcurrentHashMap<EvaluationDetailType, LongAdder> counter,
                                ConcurrentHashMap<EvaluationDetailType, ConcurrentLinkedQueue<EvaluationResultDetails>> samples){
    details.forEach(
            detail -> {
              counter.computeIfAbsent(detail.getEvaluationDetailType(), k -> new LongAdder()).increment();

              samples.putIfAbsent(detail.getEvaluationDetailType(), new ConcurrentLinkedQueue<>());
              samples.compute(detail.getEvaluationDetailType(), (type, queue) -> {
                        if(queue.size() < MAX_SAMPLE_SIZE){
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
