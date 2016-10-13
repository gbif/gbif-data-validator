package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import javax.annotation.Nullable;

/**
 * Evaluator is responsible to take a record and produce an {@link RecordEvaluationResult}.
 */
public interface RecordEvaluator {

  /**
   * Evaluate a record represented as an array of values (as String).
   *
   * @param lineNumber number of the line within the context, can be null
   * @param record     values
   *
   * @return the result of the evaluation or null if no result can be generated (e.g. empty record)
   */
  @Nullable
  RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record);

}
