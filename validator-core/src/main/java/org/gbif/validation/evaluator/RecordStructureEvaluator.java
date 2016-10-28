package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.util.List;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

/**
 * Class to evaluate the structure of a record.
 */
public class RecordStructureEvaluator implements RecordEvaluator {

  private static int expectedColumnCount;

  public RecordStructureEvaluator(List<Term> columns) {
    Validate.notNull(columns, "columns must not be null");
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
  private static RecordEvaluationResult toColumnCountMismatchResult(Long lineNumber, int expectedColumnCount,
                                                                             int actualColumnCount) {
    return new RecordEvaluationResult.Builder()
            .withLineNumber(lineNumber)
            .addBaseDetail(EvaluationType.COLUMN_MISMATCH, Integer.toString(expectedColumnCount),
                    Integer.toString(actualColumnCount))
            .build();
  }
}
