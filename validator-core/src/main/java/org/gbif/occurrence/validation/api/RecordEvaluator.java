package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import javax.annotation.Nullable;

/**
 * Evaluator is responsible to take a record and produce an {@link RecordEvaluationResult}.
 */
public interface RecordEvaluator {

  /**
   *
   * @param lineNumber number of the line within the context, can be null
   * @param record values
   * @return
   */
  RecordEvaluationResult evaluate(@Nullable Long lineNumber, String[] record);

}
