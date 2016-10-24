package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * Class to completeness of a record based on a provided fields or function.
 */
public class RecordCompletenessEvaluator implements RecordEvaluator {

  private final EvaluationType type;
  private final Term[] terms;
  private final Integer[] termIndices;

  /**
   *
   * @param type
   * @param terms
   * @param termIndices
   */
  public RecordCompletenessEvaluator(EvaluationType type, Term[] terms, Integer[] termIndices) {
    Validate.notNull(type, "EvaluationType must not be null");
    Validate.notNull(terms, "terms must not be null");
    Validate.notNull(termIndices, "termIndices must not be null");
    Validate.isTrue(terms.length == termIndices.length, "terms length must equals termIndices length");

    this.type = type;
    this.terms = terms;
    this.termIndices = termIndices;
  }

  /**
   * Creates a RecordStructureEvaluationResult instance for a mandatory value that is not provided.
   * @param lineNumber
   * @return
   */
  private RecordEvaluationResult toCompletenessResult(Long lineNumber) {
    return new RecordEvaluationResult.Builder()
            .withLineNumber(lineNumber)
            .addMissingDataDetail(type, terms)
            .build();
  }

  @Nullable
  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {

    for(int termIndex=0; termIndex< termIndices.length; termIndex++){
      if((termIndices[termIndex] < record.length) && StringUtils.isNotBlank(record[termIndices[termIndex]])){
        return null;
      }
    }
    return toCompletenessResult(lineNumber);
  }
}
