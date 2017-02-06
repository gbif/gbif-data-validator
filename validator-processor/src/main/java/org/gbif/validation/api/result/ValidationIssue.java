package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;

/**
 * Scope: EvaluationType inside a {@link ValidationResultElement}
 */
public interface ValidationIssue {

  EvaluationType getIssue();

  EvaluationCategory getIssueCategory();
}
