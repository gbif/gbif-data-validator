package org.gbif.validation.api.model;

import org.gbif.dwc.terms.Term;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a validation.
 */
public class ValidationResult {

  //public enum Status {OK, FAILED};

  //private final Status status;
  private final Boolean indexeable;

  private final FileFormat fileFormat;
  private final ValidationProfile validationProfile;

  private final Integer numberOfLines;

  //only used in case of general error with the input file
  private final ValidationErrorCode errorCode;

  private final List<DateFileValidationElement> issues;
  private final Map<Term, Long> termsFrequency;

  /**
   * Fluent builder for {@link ValidationResult}
   */
  public static class Builder {
    private Boolean indexeable;

    private FileFormat fileFormat;
    private ValidationProfile validationProfile;

    private Integer numberOfLines;

    //only used in case of general error with the input file
    private ValidationErrorCode errorCode;

    private List<DateFileValidationElement> issues = new ArrayList<>();
    private Map<Term, Long> termsFrequency;

    /**
     * Returns a Builder of {@link ValidationResult} when a validation can be performed and finished.
     *
     * @param indexeable
     * @param fileFormat
     * @param numberOfLines
     * @param validationProfile
     *
     * @return
     */
    public static Builder of(Boolean indexeable, FileFormat fileFormat, Integer numberOfLines, ValidationProfile validationProfile) {
      return new Builder(indexeable, fileFormat, numberOfLines, validationProfile);
    }

    public static Builder withError(FileFormat fileFormat, ValidationProfile validationProfile, ValidationErrorCode errorCode) {
      return new Builder(fileFormat,validationProfile, errorCode);
    }

    private Builder(Boolean indexeable, FileFormat fileFormat, Integer numberOfLines,
                    ValidationProfile validationProfile) {
      this.indexeable = indexeable;
      this.fileFormat = fileFormat;
      this.numberOfLines = numberOfLines;
      this.validationProfile = validationProfile;
    }

    /**
     * Constructor used only when we have an error (service/input error) to report.
     *
     * @param fileFormat
     * @param validationProfile
     * @param errorCode
     */
    private Builder(FileFormat fileFormat, ValidationProfile validationProfile, ValidationErrorCode errorCode) {
      this.fileFormat = fileFormat;
      this.validationProfile = validationProfile;
      this.errorCode = errorCode;
    }

    public Builder withIssues(Map<EvaluationType, Long> issueCounter,
                              Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(new DateFileValidationElement(k, v, issueSampling.get(k)))
      );
      return this;
    }

    public Builder withTermsFrequency(Map<Term, Long> termsFrequency) {
      this.termsFrequency = termsFrequency;
      return this;
    }

    public ValidationResult build() {
      return new ValidationResult(indexeable, fileFormat,
              validationProfile, numberOfLines, issues, termsFrequency, errorCode);
    }
  }

  /**
   * Use public static methods to get new instances.
   *
   * @param indexeable
   * @param fileFormat
   * @param validationProfile
   * @param issues
   * @param termsFrequency
   * @param errorCode
   */
  private ValidationResult(Boolean indexeable, FileFormat fileFormat,
                           ValidationProfile validationProfile, Integer numberOfLines,
                           List<DateFileValidationElement> issues,
                           Map<Term, Long> termsFrequency,
                           ValidationErrorCode errorCode) {
    this.indexeable = indexeable;
    this.fileFormat = fileFormat;
    this.validationProfile = validationProfile;
    this.numberOfLines = numberOfLines;
    this.issues = issues;
    this.termsFrequency = termsFrequency;
    this.errorCode = errorCode;
  }


  public Boolean isIndexeable() {
    return indexeable;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public Integer getNumberOfLines() {
    return numberOfLines;
  }

  public ValidationProfile getValidationProfile() {
    return validationProfile;
  }

  public ValidationErrorCode getErrorCode() {
    return errorCode;
  }

  public List<DateFileValidationElement> getIssues() {
    return issues;
  }

  public Map<Term, Long> getTermsFrequency() {
    return termsFrequency;
  }

  private static class DateFileValidationElement {

    private final EvaluationCategory issueCategory;
    private final EvaluationType issue;
    private final long count;
    private final List<EvaluationResultDetails> sample;

    public DateFileValidationElement(EvaluationType issue, long count, List<EvaluationResultDetails> sample) {
      this.issueCategory = issue.getCategory();
      this.issue = issue;
      this.count = count;
      this.sample = sample;
    }

    public EvaluationType getIssue() {
      return issue;
    }

    public EvaluationCategory getIssueCategory() {
      return issueCategory;
    }

    public long getCount() {
      return count;
    }

    public List<EvaluationResultDetails> getSample() {
      return sample;
    }
  }

}
