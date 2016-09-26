package org.gbif.occurrence.validation.tabular.single;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private List<RecordStructureEvaluationResult> recordStructureIssues = new ArrayList<>();
  private HashMap<OccurrenceIssue, Long> issuesCounter = new HashMap(OccurrenceIssue.values().length);

  private long recordStructureIssueCount;
  private long recordCount;

  @Override
  public void accumulate(RecordStructureEvaluationResult result) {
    recordStructureIssueCount++;

    recordStructureIssues.add(result);
  }

  @Override
  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount += 1;
    result.getDetails().forEach(
      detail -> issuesCounter.compute(detail.getIssueFlag(), (k,v) -> (v == null) ? 1 : ++v)
    );
  }

  @Override
  public List<RecordStructureEvaluationResult> getRecordStructureEvaluationResult() {
    return recordStructureIssues;
  }

  @Override
  public Map<OccurrenceIssue, Long> getAggregatedResult(){
    return issuesCounter;
  }

  @Override
  public String toString() {
    return "Record count: " + recordCount + " Issues: " + issuesCounter.toString();
  }

}
