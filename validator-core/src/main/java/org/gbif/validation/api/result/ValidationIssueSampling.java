package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationType;

import java.util.List;

/**
 * Represents the output of a specific {@link EvaluationType} with issue sampling.
 */
public class ValidationIssueSampling extends ValidationIssue {

  private final List<LineBasedEvaluationResultDetails> sample;

  ValidationIssueSampling(EvaluationType issue, long count, List<LineBasedEvaluationResultDetails> sample) {
    super(issue, count);
    this.sample = sample;
  }

  public List<LineBasedEvaluationResultDetails> getSample() {
    return sample;
  }
}
