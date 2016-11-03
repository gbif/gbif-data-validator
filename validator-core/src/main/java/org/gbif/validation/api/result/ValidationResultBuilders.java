package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.model.ValidationProfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Contains all builders for {@link ValidationResult} and different {@link ValidationResultElement} implementations.
 */
public class ValidationResultBuilders {

  /**
   * Fluent builder for {@link ValidationResult}
   */
  public static class Builder {
    private Boolean indexeable;

    private String fileName;
    private FileFormat fileFormat;
    private ValidationProfile validationProfile;

    private List<ValidationResultElement> validationResultElements;

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

    public Builder withResourceResult(ValidationResultElement validationResourceResult) {
      if(validationResultElements == null) {
        validationResultElements = new ArrayList<>();
      }
      validationResultElements.add(validationResourceResult);
      return this;
    }

    public ValidationResult build() {
      return new ValidationResult(indexeable, fileName, fileFormat, validationProfile, validationResultElements,
              errorCode);
    }
  }

  public static class RecordsValidationResultElementBuilder {
    private String fileName;
    private Long numberOfLines;
    private List<ValidationIssue> issues = new ArrayList<>();
    private Map<Term, Long> termsFrequency;
    private Map<Term, Long> interpretedValueCounts;

    public static RecordsValidationResultElementBuilder of(String fileName, Long numberOfLines){
      return new RecordsValidationResultElementBuilder(fileName, numberOfLines);
    }

    private RecordsValidationResultElementBuilder(String fileName, Long numberOfLines) {
      this.fileName = fileName;
      this.numberOfLines = numberOfLines;
    }

    public RecordsValidationResultElementBuilder withIssues(Map<EvaluationType, Long> issueCounter,
                                                             Map<EvaluationType, List<EvaluationResultDetails>> issueSampling) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(new ValidationIssueSampling(k, v, issueSampling.get(k)))
      );
      return this;
    }

    public RecordsValidationResultElementBuilder withTermsFrequency(Map<Term, Long> termsFrequency) {
      this.termsFrequency = termsFrequency;
      return this;
    }

    public RecordsValidationResultElementBuilder withInterpretedValueCounts(Map<Term, Long> interpretedValueCounts) {
      this.interpretedValueCounts = interpretedValueCounts;
      return this;
    }

    public RecordsValidationResultElement build() {
      return new RecordsValidationResultElement(fileName, numberOfLines,
              issues, termsFrequency, interpretedValueCounts);
    }
  }

  /**
   * Builder class to build a {@link DefaultValidationResultElement} instance.
   */
  public static class DefaultValidationResultElementBuilder {
    private String filename;
    private List<ValidationIssue> issues;

    public static DefaultValidationResultElementBuilder of(String filename) {
      return new DefaultValidationResultElementBuilder(filename);
    }

    private DefaultValidationResultElementBuilder(String filename) {
      this.filename = filename;
    }

    /**
     *
     * @param evaluationType
     * @param exception
     * @return
     */
    public DefaultValidationResultElementBuilder addExceptionResultDetails(EvaluationType evaluationType, String exception) {
      if(issues == null){
        issues = new ArrayList<>();
      }
      issues.add(new ExceptionResultDetails(evaluationType, exception));
      return this;
    }

    public DefaultValidationResultElement build(){
      return new DefaultValidationResultElement(this.filename, this.issues);
    }
  }

  public static class ExceptionResultDetails extends ValidationIssue {
    protected final String exception;

    ExceptionResultDetails(EvaluationType evaluationType, String exception) {
      super(evaluationType, 1l);
      this.exception = exception;
    }

    public String getException() {
      return exception;
    }
  }

}
