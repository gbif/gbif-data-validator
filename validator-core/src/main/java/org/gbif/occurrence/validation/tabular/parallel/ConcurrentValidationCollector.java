package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordEvaluationResult;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConcurrentValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private final ConcurrentHashMap<OccurrenceIssue, LongAdder> issuesCounter;

  private final ConcurrentLinkedQueue<RecordStructureEvaluationResult> recordStructureEvaluationIssue;
  private final LongAdder recordStructureIssueCount;

  private final LongAdder recordCount;

  public ConcurrentValidationCollector(){
    issuesCounter = new ConcurrentHashMap(OccurrenceIssue.values().length);
    recordStructureEvaluationIssue = new ConcurrentLinkedQueue();

    recordStructureIssueCount = new LongAdder();
    recordCount = new LongAdder();
  }

  @Override
  public void accumulate(RecordEvaluationResult result) {
    switch (result.getEvaluationType()) {
      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
        break;
      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretionBasedEvaluationResult)result);
        break;
    }
  }

  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount.increment();
    result.getDetails().forEach(
            details -> issuesCounter.computeIfAbsent(details.getIssueFlag(), k -> new LongAdder()).increment()
    );
  }

  private void accumulate(RecordStructureEvaluationResult result) {
    recordStructureIssueCount.increment();
    recordStructureEvaluationIssue.add(result);
  }

  @Override
  public List<RecordStructureEvaluationResult> getRecordStructureEvaluationResult() {
    return null;
  }

  @Override
  public Map<OccurrenceIssue, Long> getAggregatedResult() {
    return issuesCounter.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
                                                                      entry -> entry.getValue().longValue()));
  }

  @Override
  public String toString() {
    return "ConcurrentValidationCollector{" +
           "issuesCounter=" + issuesCounter +
           ", recordCount=" + recordCount +
           '}';
  }
}
