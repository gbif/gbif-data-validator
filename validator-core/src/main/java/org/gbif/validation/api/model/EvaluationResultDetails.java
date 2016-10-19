package org.gbif.validation.api.model;

import java.io.Serializable;

/**
 * For each different type of evaluation result, this represents the "contract" of the details of the result.
 */
public interface EvaluationResultDetails extends Serializable {
  EvaluationType getEvaluationType();
}
