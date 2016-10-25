package org.gbif.validation.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a validation.
 */
public class ValidationResult {

  public enum Status {OK, FAILED};

  private final Status status;
  private final Boolean indexeable;

  private final FileFormat fileFormat;
  private final ValidationProfile validationProfile;

  private final Integer numberOfLines;

  //only used in case of general error with the input file
  private final ValidationErrorCode errorCode;

  private final List<DateFileValidationElement> issues = new ArrayList<>();

  /**
   * Use public static methods to get new instances.
   *
   * @param status
   * @param indexeable
   * @param fileFormat
   * @param validationProfile
   * @param issueCounter
   * @param issueSampling
   * @param errorCode
   */
  private ValidationResult(Status status, Boolean indexeable, FileFormat fileFormat,
                          ValidationProfile validationProfile, Integer numberOfLines, Map<EvaluationType, Long> issueCounter,
                          Map<EvaluationType, List<EvaluationResultDetails>> issueSampling, ValidationErrorCode errorCode) {
    this.status = status;
    this.indexeable = indexeable;
    this.fileFormat = fileFormat;
    this.validationProfile = validationProfile;
    this.numberOfLines = numberOfLines;
    this.errorCode = errorCode;

    issueCounter.forEach(
            (k, v) ->
                    issues.add(new DateFileValidationElement(k, v, issueSampling.get(k)))
    );
  }

  /**
   * Returns a new instance of {@link ValidationResult} when a validation can be performed and finished.
   *
   * @param status
   * @param indexeable
   * @param fileFormat
   * @param validationProfile
   * @param issueCounter
   * @param issueSampling
   * @return
   */
  public static ValidationResult of(Status status, boolean indexeable, FileFormat fileFormat,
                                    ValidationProfile validationProfile, Integer numberOfRecords,
                                    Map<EvaluationType, Long> issueCounter,
                                    Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
    return new ValidationResult(status, indexeable ,fileFormat, validationProfile, numberOfRecords,
            issueCounter, issueSampling, null);
  }

  /**
   * Returns a new instance of {@link ValidationResult} when a validation can NOT be performed.
   *
   * @param status
   * @param fileFormat
   * @param errorCode
   * @return
   */
  public static ValidationResult withError(Status status, FileFormat fileFormat, ValidationErrorCode errorCode) {
    return new ValidationResult(status, null, fileFormat, null, null, null, null, errorCode);
  }

  public Status getStatus() {
    return status;
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

  public List<DateFileValidationElement> getIssues(){
    return issues;
  }

  private static class DateFileValidationElement {

    private final EvaluationCategory issueCategory;
    private final EvaluationType issue;
    private final long count;
    private final List<EvaluationResultDetails> sample;

    public DateFileValidationElement(EvaluationType issue, long count, List<EvaluationResultDetails> sample){
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
