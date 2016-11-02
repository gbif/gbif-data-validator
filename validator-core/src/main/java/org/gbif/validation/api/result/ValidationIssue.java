package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationResultDetails;
import org.gbif.validation.api.model.EvaluationType;

import java.util.List;

/**
 * Represents the output of a specific {@link EvaluationType}.
 *
 * Immutable class
 */
public class ValidationIssue {

  private final EvaluationType issue;
  private final long count;
  private final List<EvaluationResultDetails> sample;

  ValidationIssue(EvaluationType issue, long count, List<EvaluationResultDetails> sample) {
    this.issue = issue;
    this.count = count;
    this.sample = sample;
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

  public List<EvaluationResultDetails> getSample() {
    return sample;
  }
}
