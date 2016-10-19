package org.gbif.validation.api.model;

import org.gbif.validation.api.RecordEvaluator;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Decorator around {@link RecordEvaluator} to run a validation chain and return the result as a single
 * {@link RecordEvaluationResult}.
 */
public class RecordEvaluatorChain implements RecordEvaluator {

  private final List<RecordEvaluator> evaluators;

  public RecordEvaluatorChain(List<RecordEvaluator> evaluators) {
    this.evaluators = evaluators;
  }

  @Nullable
  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {
    RecordEvaluationResult combinedResult = null;
    for(RecordEvaluator evaluator : evaluators) {
      combinedResult = RecordEvaluationResult.Builder.merge(combinedResult, evaluator.evaluate(lineNumber, record));
    }
    return combinedResult;
  }
}
