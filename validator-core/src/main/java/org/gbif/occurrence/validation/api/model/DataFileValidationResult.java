package org.gbif.occurrence.validation.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a DataFile validation
 */
public class DataFileValidationResult {

  public enum Status {OK, FAILED};

  private final Status status;
  private final boolean indexeable;

  //only used in case of general error with the input file
  private final String error;

  private final List<DateFileValidationElement> issues = new ArrayList<>();


  public DataFileValidationResult(Status status, boolean indexeable, Map<EvaluationType, Long> issueCounter,
                                  Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
    this.status = status;
    this.indexeable = indexeable;
    this.error = null;

    issueCounter.forEach(
            (k, v) ->
                    issues.add(new DateFileValidationElement(k, v, issueSampling.get(k)))
    );
  }

  public Status getStatus() {
    return status;
  }

  public boolean isIndexeable() {
    return indexeable;
  }

  public String getError() {
    return error;
  }

  public List<DateFileValidationElement> getIssues(){
    return issues;
  }

  private static class DateFileValidationElement {

    private EvaluationType issue;
    private long count;
    private List<EvaluationResultDetails> sample;

    public DateFileValidationElement(EvaluationType issue, long count, List<EvaluationResultDetails> sample){
      this.issue = issue;
      this.count = count;
      this.sample = sample;
    }

    public EvaluationType getIssue() {
      return issue;
    }

    public long getCount() {
      return count;
    }

    public List<EvaluationResultDetails> getSample() {
      return sample;
    }
  }

}
