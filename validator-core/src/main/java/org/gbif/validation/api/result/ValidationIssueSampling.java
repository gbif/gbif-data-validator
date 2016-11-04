package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationType;

import java.util.List;

/**
 * Represents the output of a specific {@link EvaluationType} with issue sampling.
 */
public class ValidationIssueSampling extends ValidationIssue {

  private final List<EvaluationResultDetails> sample;

  ValidationIssueSampling(EvaluationType issue, long count, List<EvaluationResultDetails> sample) {
    super(issue, count);
    this.sample = sample;
  }

  public List<EvaluationResultDetails> getSample() {
    return sample;
  }
}
