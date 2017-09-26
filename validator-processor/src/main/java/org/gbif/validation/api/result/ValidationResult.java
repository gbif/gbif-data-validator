package org.gbif.validation.api.result;

import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.vocabulary.FileFormat;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;


/**
 * Contains the result of a validation. This is the top class of validation result.
 * Scope: the entire resource submitted.
 */
public class ValidationResult implements Serializable {

  //private final Status status;
  private final Boolean indexeable;

  private final String fileName;
  private final String receivedMediaType;
  private final FileFormat fileFormat;
  private final ValidationProfile validationProfile;

  //only used in case of general error with the input file
  private final ValidationErrorCode errorCode;
  private final String errorMessage;

  private final List<ValidationResultElement> results;

  /**
   * Creates a {@link ValidationResult} after an error occurred.
   *
   * @param fileName
   * @param fileFormat can be null
   * @param receivedMediaType can be null
   * @param errorCode
   * @param errorMessage can be null
   *
   * @return new {@link ValidationResult} instance
   */
  public static ValidationResult onError(String fileName, @Nullable FileFormat fileFormat,
                                         @Nullable String receivedMediaType,
                                         ValidationErrorCode errorCode, @Nullable String errorMessage) {
    return new ValidationResult(false, fileName, fileFormat, receivedMediaType, null, null, errorCode, errorMessage);
  }

  /**
   *
   * @param indexeable
   * @param fileName
   * @param receivedMediaType
   * @param fileFormat
   * @param validationProfile
   * @param results
   */
  public ValidationResult(Boolean indexeable, String fileName, FileFormat fileFormat, String receivedMediaType,
                          ValidationProfile validationProfile, List<ValidationResultElement> results) {
    this(indexeable, fileName, fileFormat, receivedMediaType, validationProfile, results, null, null);
  }

  private ValidationResult(Boolean indexeable, String fileName, FileFormat fileFormat, String receivedMediaType,
                           ValidationProfile validationProfile, List<ValidationResultElement> results,
                           ValidationErrorCode errorCode, String errorMessage) {
    this.indexeable = indexeable;
    this.fileName = fileName;
    this.fileFormat = fileFormat;
    this.receivedMediaType = receivedMediaType;
    this.validationProfile = validationProfile;
    this.results = results;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public Boolean isIndexeable() {
    return indexeable;
  }

  public String getFileName() {
    return fileName;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public ValidationProfile getValidationProfile() {
    return validationProfile;
  }

  public List<ValidationResultElement> getResults() {
    return results;
  }

  public ValidationErrorCode getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getReceivedMediaType() {
    return receivedMediaType;
  }

}
