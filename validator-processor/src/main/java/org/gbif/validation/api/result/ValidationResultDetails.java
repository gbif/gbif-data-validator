package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;

import java.util.Map;

/**
 * This class shares some concepts with RecordEvaluationResultDetails but is not used in the same context.
 * To separate the concerns we decided to have 2 classes.
 *
 * Scope : single line of a rowType
 */
public class ValidationResultDetails {

  private final Long lineNumber;
  private final String recordId;

  private final Map<Term, String> relatedData;
  private final String expected;
  private final String found;

  /**
   * Used to create a {@link ValidationResultDetails} that does not contain any details except the id of the record.
   * @param recordId
   * @return
   */
  public static ValidationResultDetails recordIdOnly(String recordId) {
    return new ValidationResultDetails(null, recordId, null, null, null);
  }

  public ValidationResultDetails(Long lineNumber, String recordId, String expected, String found,
                                 Map<Term, String> relatedData) {
    this.recordId = recordId;
    this.lineNumber = lineNumber;
    this.expected = expected;
    this.found = found;
    this.relatedData = relatedData;
  }

  public String getRecordId() {
    return recordId;
  }

  public Long getLineNumber() {
    return lineNumber;
  }

  public Map<Term, String> getRelatedData() {
    return relatedData;
  }

  public String getExpected() {
    return expected;
  }

  public String getFound() {
    return found;
  }
}
