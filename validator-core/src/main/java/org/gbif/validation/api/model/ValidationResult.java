package org.gbif.validation.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a validation.
 */
public class ValidationResult {

  public enum Status {OK, FAILED};

  private final Status status;
  private final boolean indexeable;

  private final ValidationProfile validationProfile = ValidationProfile.GBIF_INDEXING_PROFILE;

  //only used in case of general error with the input file
  private final String error;

  private final List<DateFileValidationElement> issues = new ArrayList<>();


  public ValidationResult(Status status, boolean indexeable,
                          Map<EvaluationType, Long> issueCounter,
                          Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
    this.status = status;
    this.indexeable = indexeable;
    //FIXME
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

  public ValidationProfile getValidationProfile() {
    return validationProfile;
  }

  public String getError() {
    return error;
  }

  public List<DateFileValidationElement> getIssues(){
    return issues;
  }

  private static class DateFileValidationElement {

    private final EvaluationCategory issueCategory;
    private final EvaluationType issue;
    private final long count;
    private final List<EvaluationResultDetails> sample;

    public DateFileValidationElement(EvaluationType issue, long count, List<EvaluationResultDetails> sample){
      this.issueCategory = issue.getCategory();
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
