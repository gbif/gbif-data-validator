package org.gbif.occurrence.validation.api.model;

/**
 * For each different type of evaluation result, this represents the "contract" of the details of the result.
 */
public interface EvaluationResultDetails {
  EvaluationType getEvaluationType();
}
