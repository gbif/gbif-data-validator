package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;

/**
 * A {@link ValidationResultElement} represents an element in the resource in validation.
 * For DarwinCore Archive this could be the meta.xml, a core file or an extension file.
 *
 * Scope: part (per rowType) of the resource submitted.
 *
 */
public class ValidationResultElement implements Serializable {

  private final String fileName;
  private final List<ValidationIssue> issues;

  private final Long numberOfLines;
  private final DwcFileType fileType;
  //From Dwc "class of data represented by each row"
  private final Term rowType;

  private final Map<Term, Long> termsFrequency;
  private final Map<Term, Long> interpretedValueCounts;

  /**
   * Get a new {@link ValidationResultElement} that represents an exception linked to a {@link EvaluationType}.
   *
   * @param fileName
   * @param evaluationType
   * @param exception
   *
   * @return
   */
  public static ValidationResultElement onException(String fileName, EvaluationType evaluationType, String exception){
    //EvaluationType evaluationType
    List<ValidationIssue> issues = new ArrayList<>();
    issues.add(ValidationIssues.withException(evaluationType, exception));
    return new ValidationResultElement(fileName, null, null, null, issues);
  }

  public ValidationResultElement(String fileName, Long numberOfLines, DwcFileType fileType, Term rowType,
                                 List<ValidationIssue> issues){
    this(fileName, numberOfLines, fileType, rowType, issues, null, null);
  }

  /**
   *
   * @param fileName
   * @param numberOfLines total number of line, including the header line
   * @param fileType
   * @param rowType
   * @param issueCounter
   * @param issueSampling
   * @param termsFrequency
   * @param interpretedValueCounts
   */
  public ValidationResultElement(String fileName, Long numberOfLines, DwcFileType fileType, Term rowType,
                          Map<EvaluationType, Long> issueCounter,
                          Map<EvaluationType, List<ValidationResultDetails>> issueSampling,
                          Map<Term, Long> termsFrequency,
                          Map<Term, Long> interpretedValueCounts) {
    this(fileName, numberOfLines, fileType, rowType, new ArrayList<>(), termsFrequency, interpretedValueCounts);

    if(issueCounter != null && issueSampling != null) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(ValidationIssues.withSample(k, v, issueSampling.get(k))));
    }
  }

  /**
   * Full constructor
   *
   * @param fileName
   * @param numberOfLines
   * @param fileType
   * @param rowType
   * @param issues
   * @param termsFrequency
   * @param interpretedValueCounts
   */
  public ValidationResultElement(String fileName, Long numberOfLines, DwcFileType fileType, Term rowType,
                                 List<ValidationIssue> issues,
                                 Map<Term, Long> termsFrequency,
                                 Map<Term, Long> interpretedValueCounts) {
    this.fileName = fileName;
    this.numberOfLines = numberOfLines;
    this.fileType = fileType;
    this.rowType = rowType;
    this.issues = issues;
    this.termsFrequency = termsFrequency;
    this.interpretedValueCounts = interpretedValueCounts;
  }


  public String getFileName() {
    return fileName;
  }

  public Long getNumberOfLines() {
    return numberOfLines;
  }

  public DwcFileType getFileType() {
    return fileType;
  }

  public Term getRowType() {
    return rowType;
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

  public Map<Term, Long> getTermsFrequency() {
    return termsFrequency;
  }

  public Map<Term, Long> getInterpretedValueCounts() {
    return interpretedValueCounts;
  }


  /**
   * Check if the list of issue contains at least one issue of the provided {@link EvaluationCategory}.
   * @param evaluationCategory
   * @return
   */
  public boolean contains(EvaluationCategory evaluationCategory) {
    if(issues != null) {
      return issues.stream()
              .filter( vi -> evaluationCategory.equals(vi.getIssue().getCategory()))
              .findAny().isPresent();
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("fileName", fileName)
            .add("fileType", fileType)
            .add("rowType", rowType)
            .add("issues", issues)
            .add("numberOfLines", numberOfLines)
            .add("termsFrequency", termsFrequency)
            .add("interpretedValueCounts", interpretedValueCounts)
            .toString();
  }

}
