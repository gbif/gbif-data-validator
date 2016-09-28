package org.gbif.occurrence.validation.model;

import org.gbif.api.vocabulary.EvaluationDetailType;

/**
 * Details of an EvaluationResult.
 */
public interface EvaluationResultDetails {

  EvaluationDetailType getEvaluationDetailType();

}

