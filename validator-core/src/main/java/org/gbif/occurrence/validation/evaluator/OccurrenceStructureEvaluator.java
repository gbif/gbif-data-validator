package org.gbif.occurrence.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.api.model.EvaluationType;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import java.text.MessageFormat;
import java.util.Map;
import javax.annotation.Nullable;

public class OccurrenceStructureEvaluator implements RecordEvaluator {

  private String[] fields;

  public OccurrenceStructureEvaluator(String[] fields) {
    this.fields = fields;
  }

  @Override
  public RecordEvaluationResult process(
          @Nullable Long lineNumber, Map<Term, String> record) {
    int expectedColumnCount = getFields().length;
    if (record.size() != expectedColumnCount) {
      return toColumnCountMismatchResult(lineNumber, expectedColumnCount, record.size());
    }
    return null;
  }

  @Override
  public String[] getFields() {
    return fields;
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
            .addDescription(EvaluationType.COLUMN_MISMATCH, MessageFormat.format("Column count mismatch: expected {0} columns, got {1} columns",
                    expectedColumnCount, actualColumnCount)).build();
  }
}
