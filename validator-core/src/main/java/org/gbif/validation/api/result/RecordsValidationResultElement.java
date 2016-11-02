package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

/**
 * {@link ValidationResultElement} implementation representing results at the record level.
 */
public class RecordsValidationResultElement extends DefaultValidationResultElement {

  private final Long numberOfLines;

  private final Map<Term, Long> termsFrequency;
  private final Map<Term, Long> interpretedValueCounts;

  RecordsValidationResultElement(String fileName, Long numberOfLines, List<ValidationIssue> issues,
                                          Map<Term, Long> termsFrequency, Map<Term, Long> interpretedValueCounts){
    super(fileName, issues);
    this.numberOfLines = numberOfLines;
    this.termsFrequency = termsFrequency;
    this.interpretedValueCounts = interpretedValueCounts;
  }

  public Long getNumberOfLines() {
    return numberOfLines;
  }

  public Map<Term, Long> getTermsFrequency() {
    return termsFrequency;
  }

  public Map<Term, Long> getInterpretedValueCounts() {
    return interpretedValueCounts;
  }
}
