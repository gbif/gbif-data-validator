package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

/**
 * {@link RecordsValidationResultElement} contains results of the evaluation of the
 * data (record) of the resource.
 */
public class RecordsValidationResultElement extends DefaultValidationResultElement {

  private final Long numberOfLines;
  //From Dwc "class of data represented by each row"
  private final Term rowType;

  private final Map<Term, Long> termsFrequency;
  private final Map<Term, Long> interpretedValueCounts;

  RecordsValidationResultElement(String fileName, Long numberOfLines, Term rowType,
                                 List<ValidationIssue> issues,
                                 Map<Term, Long> termsFrequency, Map<Term, Long> interpretedValueCounts){
    super(fileName, issues);
    this.numberOfLines = numberOfLines;
    this.rowType = rowType;
    this.termsFrequency = termsFrequency;
    this.interpretedValueCounts = interpretedValueCounts;
  }

  public Long getNumberOfLines() {
    return numberOfLines;
  }

  public Term getRowType() {
    return rowType;
  }

  public Map<Term, Long> getTermsFrequency() {
    return termsFrequency;
  }

  public Map<Term, Long> getInterpretedValueCounts() {
    return interpretedValueCounts;
  }
}
