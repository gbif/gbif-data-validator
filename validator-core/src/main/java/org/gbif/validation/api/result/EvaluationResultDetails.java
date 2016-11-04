package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;

/**
 * For each different type of evaluation result, this represents the "contract" of the details of the result.
 */
public interface EvaluationResultDetails extends Serializable {
  EvaluationType getEvaluationType();
}
