package org.gbif.validation.api.model;

import org.gbif.dwc.terms.Term;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a validation.
 */
public class ValidationResult implements Serializable {

  //private final Status status;
  private final Boolean indexeable;

  private final String fileName;
  private final FileFormat fileFormat;
  private final ValidationProfile validationProfile;

  //only used in case of general error with the input file
  private final ValidationErrorCode errorCode;

  //TODO maybe we should store the concrete type to allow typed getter?
  private final List<ValidationResourceResult> results;

  /**
   * Fluent builder for {@link ValidationResult}
   */
  public static class Builder {
    private Boolean indexeable;

    private String fileName;
    private FileFormat fileFormat;
    private ValidationProfile validationProfile;

    private List<ValidationResourceResult> recordsValidationResourceResults;

    //only used in case of general error with the input file
    private ValidationErrorCode errorCode;


    /**
     * Returns a Builder of {@link ValidationResult} when a validation can be performed and finished.
     *
     * @param indexeable
     * @param fileFormat
     * @param validationProfile
     *
     * @return
     */
    public static Builder of(Boolean indexeable, String fileName, FileFormat fileFormat, ValidationProfile validationProfile) {
      return new Builder(indexeable, fileName, fileFormat, validationProfile);
    }

    public static Builder withError(FileFormat fileFormat, ValidationProfile validationProfile, ValidationErrorCode errorCode) {
      return new Builder(fileFormat,validationProfile, errorCode);
    }

    private Builder(Boolean indexeable, String fileName, FileFormat fileFormat, ValidationProfile validationProfile) {
      this.indexeable = indexeable;
      this.fileName = fileName;
      this.fileFormat = fileFormat;
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

    public Builder withResourceResult(RecordsValidationResourceResult recordsValidationResourceResult) {
      if(recordsValidationResourceResults == null) {
        recordsValidationResourceResults = new ArrayList<>();
      }
      recordsValidationResourceResults.add(recordsValidationResourceResult);
      return this;
    }

    public ValidationResult build() {
      return new ValidationResult(indexeable, fileName, fileFormat, validationProfile, recordsValidationResourceResults,
              errorCode);
    }
  }

  public static class RecordsValidationResourceResultBuilder {
    private String fileName;
    private Long numberOfLines;
    private List<DateFileValidationElement> issues = new ArrayList<>();
    private Map<Term, Long> termsFrequency;
    private Map<Term, Long> interpretedValueCounts;

    public static RecordsValidationResourceResultBuilder of(String fileName, Long numberOfLines){
      return new RecordsValidationResourceResultBuilder(fileName, numberOfLines);
    }

    private RecordsValidationResourceResultBuilder(String fileName, Long numberOfLines) {
      this.fileName = fileName;
      this.numberOfLines = numberOfLines;
    }

    public RecordsValidationResourceResultBuilder withIssues(Map<EvaluationType, Long> issueCounter,
                              Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(new DateFileValidationElement(k, v, issueSampling.get(k)))
      );
      return this;
    }

    public RecordsValidationResourceResultBuilder withTermsFrequency(Map<Term, Long> termsFrequency) {
      this.termsFrequency = termsFrequency;
      return this;
    }

    public RecordsValidationResourceResultBuilder withInterpretedValueCounts(Map<Term, Long> interpretedValueCounts) {
      this.interpretedValueCounts = interpretedValueCounts;
      return this;
    }

    public RecordsValidationResourceResult build() {
      return new RecordsValidationResourceResult(fileName, numberOfLines,
              issues, termsFrequency, interpretedValueCounts);
    }

  }

  /**
   * Use public static methods to get new instances.
   *
   * @param indexeable
   * @param fileFormat
   * @param validationProfile
   * @param errorCode
   */
  private ValidationResult(Boolean indexeable, String fileName, FileFormat fileFormat,
                           ValidationProfile validationProfile,
                           List<ValidationResourceResult> results,
                           ValidationErrorCode errorCode) {
    this.indexeable = indexeable;
    this.fileName = fileName;
    this.fileFormat = fileFormat;
    this.validationProfile = validationProfile;
    this.results = results;
    this.errorCode = errorCode;
  }

  public Boolean isIndexeable() {
    return indexeable;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public ValidationProfile getValidationProfile() {
    return validationProfile;
  }

  public List<ValidationResourceResult> getResults() {
    return results;
  }

  public ValidationErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Contract of a {@link ValidationResourceResult}.
   */
  private interface ValidationResourceResult {
    List<DateFileValidationElement> getIssues();
  }

  /**
   * {@link ValidationResourceResult} represents the result one of possibly multiple resources
   * included in the validation.
   * This class is Immutable
   */
  public static class RecordsValidationResourceResult implements ValidationResourceResult {
    private final String fileName;
    private final Long numberOfLines;

    private final List<DateFileValidationElement> issues;
    private final Map<Term, Long> termsFrequency;
    private final Map<Term, Long> interpretedValueCounts;

    private RecordsValidationResourceResult(String fileName, Long numberOfLines, List<DateFileValidationElement> issues,
                                            Map<Term, Long> termsFrequency, Map<Term, Long> interpretedValueCounts){
      this.fileName = fileName;
      this.numberOfLines = numberOfLines;
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

    public List<DateFileValidationElement> getIssues() {
      return issues;
    }

    public Map<Term, Long> getTermsFrequency() {
      return termsFrequency;
    }

    public Map<Term, Long> getInterpretedValueCounts() {
      return interpretedValueCounts;
    }
  }

  /**
   * This class is Immutable
   */
  private static class DateFileValidationElement {

    private final EvaluationCategory issueCategory;
    private final EvaluationType issue;
    private final long count;
    private final List<EvaluationResultDetails> sample;

    private DateFileValidationElement(EvaluationType issue, long count, List<EvaluationResultDetails> sample) {
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
