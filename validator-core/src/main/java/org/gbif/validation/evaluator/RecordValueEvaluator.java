package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.util.function.Function;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

/**
 * Class to evaluate the value(s) inside an Occurrence record based on a provided function.
 * Currently limited to a single field.
 */
public class RecordValueEvaluator implements RecordEvaluator {

  private final EvaluationType type;
  private final Function<String, Boolean> valueCheck;
  private final Term term;
  private final int termIndex;

  /**
   *
   * @param type the EvaluationType this evaluator represents
   * @param valueCheck the function to check if the value is "valid" or not
   * @param term the Term of the value to check. Used to generate the RecordEvaluationResult.
   * @param termIndex the index of the value to check in the record
   */
  public RecordValueEvaluator(EvaluationType type, Function<String, Boolean> valueCheck, Term term,
                              int termIndex) {
    Validate.notNull(type, "EvaluationType must not be null");
    Validate.notNull(valueCheck, "valueCheck function must not be null");
    Validate.notNull(term, "Term must not be null");

    this.type = type;
    this.valueCheck = valueCheck;
    this.term = term;
    this.termIndex = termIndex;
  }

  /**
   * Creates a RecordStructureEvaluationResult instance for a mandatory value that is not provided.
   * @param lineNumber
   * @param term
   * @return
   */
  private RecordEvaluationResult toTermValueResult(Long lineNumber, Term term) {
    return new RecordEvaluationResult.Builder()
            .withLineNumber(lineNumber)
            .addTermValueDetail(type, term)
            .build();
  }

  @Nullable
  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {
    if(!valueCheck.apply(termIndex > record.length ? null : record[termIndex])){
      return toTermValueResult(lineNumber, term);
    }
    return null;
  }
}
