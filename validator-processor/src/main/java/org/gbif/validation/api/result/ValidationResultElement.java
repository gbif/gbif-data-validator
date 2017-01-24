package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link ValidationResultElement} represents an element in the resource in validation.
 * For DarwinCore Archive this could be the meta.xml, a core file or an extension file.
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
    issues.add(new ValidationIssue(evaluationType, 1l, exception));
    return new ValidationResultElement(fileName, null, null, null, issues);
  }

  public ValidationResultElement(String fileName, Long numberOfLines, DwcFileType fileType, Term rowType,
                                 List<ValidationIssue> issues){
    this(fileName, numberOfLines, fileType, rowType, issues, null, null);
  }

  public ValidationResultElement(String fileName, Long numberOfLines, DwcFileType fileType, Term rowType,
                          Map<EvaluationType, Long> issueCounter,
                          Map<EvaluationType, List<ValidationResultDetails>> issueSampling,
                          Map<Term, Long> termsFrequency,
                          Map<Term, Long> interpretedValueCounts) {
    this(fileName, numberOfLines, fileType, rowType, new ArrayList<>(), termsFrequency, interpretedValueCounts);

    if(issueCounter != null && issueSampling != null) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(new ValidationIssue(k, v, issueSampling.get(k))));
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
   * Represents the output of a specific {@link EvaluationType}.
   *
   * Immutable class
   */
  public static class ValidationIssue implements Serializable {

    private final EvaluationType issue;
    private final long count;
    private final List<ValidationResultDetails> sample;
    private final String exception;

    ValidationIssue(EvaluationType issue, long count) {
      this(issue, count, null, null);
    }

    ValidationIssue(EvaluationType issue, long count, String exception) {
      this(issue, count, null, exception);
    }

    ValidationIssue(EvaluationType issue, long count, List<ValidationResultDetails> sample) {
      this(issue, count, sample, null);
    }

    ValidationIssue(EvaluationType issue, long count, List<ValidationResultDetails> sample, String exception) {
      this.issue = issue;
      this.count = count;
      this.sample = sample;
      this.exception = exception;
    }

    public EvaluationType getIssue() {
      return issue;
    }

    public EvaluationCategory getIssueCategory() {
      if(issue == null) {
        return null;
      }
      return issue.getCategory();
    }

    public long getCount() {
      return count;
    }

    @Nullable
    public List<ValidationResultDetails> getSample() {
      return sample;
    }

    public String getException() {
      return exception;
    }
  }


}
