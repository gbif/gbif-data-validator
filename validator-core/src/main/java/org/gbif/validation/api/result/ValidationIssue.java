package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;

/**
 * Represents the output of a specific {@link EvaluationType} with no details.
 *
 * Immutable class
 */
public class ValidationIssue implements Serializable {

  private final EvaluationType issue;
  private final long count;


  ValidationIssue(EvaluationType issue, long count) {
    this.issue = issue;
    this.count = count;
  }

  public EvaluationType getIssue() {
    return issue;
  }

  public EvaluationCategory getIssueCategory() {
    if(issue == null) {
      return null;
    }
    return issue.getCategory();
  }

  public long getCount() {
    return count;
  }


}
