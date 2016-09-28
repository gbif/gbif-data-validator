package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.EvaluationResult;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

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

  private final ConcurrentHashMap<EvaluationDetailType, LongAdder> issuesCounter;

  private final ConcurrentLinkedQueue<RecordStructureEvaluationResult> recordStructureEvaluationIssue;

  //private ConcurrentHashMap<EvaluationType, ConcurrentHashMap<EvaluationDetailType, Long>> issueCounter = new HashMap<>();
  //private ConcurrentHashMap<EvaluationType, ConcurrentHashMap<EvaluationDetailType, ConcurrentLinkedQueue<EvaluationResultDetails>>> issueSampling = new HashMap<>();

  private final LongAdder recordStructureIssueCount;

  private final LongAdder recordCount;

  public ConcurrentValidationCollector() {
    issuesCounter = new ConcurrentHashMap<>(OccurrenceIssue.values().length);
    recordStructureEvaluationIssue = new ConcurrentLinkedQueue<>();

    recordStructureIssueCount = new LongAdder();
    recordCount = new LongAdder();
  }

  public void accumulate(EvaluationResult result) {
    switch (result.getEvaluationType()) {
      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
        break;
      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretionBasedEvaluationResult)result);
        break;
    }
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, Long>> getAggregatedCounts() {
    //TODO C.G. not implemented
    return null;
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> getSamples() {
    //TODO C.G. not implemented
    return null;
  }


  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount.increment();
    result.getDetails().forEach(
            details -> issuesCounter.computeIfAbsent(details.getEvaluationDetailType(), k -> new LongAdder()).increment()
    );
  }

  public void accumulate(RecordStructureEvaluationResult result) {
    recordStructureIssueCount.increment();
    recordStructureEvaluationIssue.add(result);
  }

  @Override
  public String toString() {
    return "ConcurrentValidationCollector{" +
           "issuesCounter=" + issuesCounter +
           ", recordCount=" + recordCount +
           '}';
  }
}
