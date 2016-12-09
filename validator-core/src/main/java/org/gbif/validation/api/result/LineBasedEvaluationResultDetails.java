package org.gbif.validation.api.result;

/**
 * More specific definition of {@link EvaluationResultDetails}.
 */
public interface LineBasedEvaluationResultDetails extends EvaluationResultDetails {
  Long getLineNumber();
}
