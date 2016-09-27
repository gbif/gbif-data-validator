package org.gbif.occurrence.validation.api;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class DataFileValidationResult {

  private List<RecordStructureEvaluationResult> recordStructureIssue;
  private Map<OccurrenceIssue, Long> issues;

  public DataFileValidationResult(Map<OccurrenceIssue, Long> issues,
                                  List<RecordStructureEvaluationResult> recordStructureIssue) {
    this.recordStructureIssue = recordStructureIssue;
    this.issues = issues;
  }

  public Map<OccurrenceIssue, Long> getIssues() {
    return issues;
  }

  public void setIssues(Map<OccurrenceIssue, Long> issues) {
    this.issues = issues;
  }

  @Override
  public String toString() {

    String toString = "DataFileValidationResult{" +
            "issues=" + issues;

    //temporary code, will be replaced by json serialisation
    if(recordStructureIssue != null && !recordStructureIssue.isEmpty()){
      toString += ", recordStructureIssue{[";
      for(RecordStructureEvaluationResult result : recordStructureIssue){
        toString += "{lineNumber:"+result.getRecordId() + ", " + result.getDetails()+"}, ";
      }

      toString = StringUtils.removeEnd(toString, ",");
      toString += "]}";
    }

    toString += '}';

    return toString;
  }
}
