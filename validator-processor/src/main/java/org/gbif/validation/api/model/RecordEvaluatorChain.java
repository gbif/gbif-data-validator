package org.gbif.validation.api.model;

import org.gbif.validation.api.RecordEvaluator;

import java.util.List;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

/**
 * Decorator around {@link RecordEvaluator} to run a validation chain and return the result as a single
 * {@link RecordEvaluationResult}.
 */
public class RecordEvaluatorChain implements RecordEvaluator {

  private final List<RecordEvaluator> evaluators;

  /**
   * Creates a new RecordEvaluatorChain from a list of {@link RecordEvaluator}.
   *
   * @param evaluators
   */
  public RecordEvaluatorChain(List<RecordEvaluator> evaluators) {
    Validate.notNull(evaluators, "evaluators can not be null");
    this.evaluators = evaluators;
  }

  @Nullable
  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable List<String> record) {
    RecordEvaluationResult combinedResult = null;
    for(RecordEvaluator evaluator : evaluators) {
      combinedResult = RecordEvaluationResult.Builder.merge(combinedResult, evaluator.evaluate(lineNumber, record));
    }
    return combinedResult;
  }
}
