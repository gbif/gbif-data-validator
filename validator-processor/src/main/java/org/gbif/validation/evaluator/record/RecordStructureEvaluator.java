package org.gbif.validation.evaluator.record;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.Validate;

/**
 * Class to evaluate the structure of a record.
 */
@ThreadSafe
public class RecordStructureEvaluator implements RecordEvaluator {

  private final Term rowType;
  private final int expectedColumnCount;

  /**
   *
   * @param rowType rowType on which this RecordEvaluator will operate.
   * @param columns
   */
  public RecordStructureEvaluator(Term rowType, List<Term> columns) {
    Validate.notNull(columns, "columns must not be null");
    this.rowType = rowType;
    expectedColumnCount = columns.size();
  }

  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, String[] record) {
    if (record.length != expectedColumnCount) {
      return toColumnCountMismatchResult(lineNumber, expectedColumnCount, record.length);
    }
    return null;
  }

  /**
   * Creates a RecordStructureEvaluationResult instance for a column count mismatch.
   *
   * @param lineNumber
   * @param expectedColumnCount
   * @param actualColumnCount
   * @return
   */
  private RecordEvaluationResult toColumnCountMismatchResult(Long lineNumber, int expectedColumnCount,
                                                                    int actualColumnCount) {
    return RecordEvaluationResult.Builder.of(rowType, lineNumber)
            .addBaseDetail(EvaluationType.COLUMN_MISMATCH, Integer.toString(expectedColumnCount),
                    Integer.toString(actualColumnCount))
            .build();
  }
}
