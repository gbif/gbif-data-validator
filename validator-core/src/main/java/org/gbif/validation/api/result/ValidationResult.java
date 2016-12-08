package org.gbif.validation.api.result;

import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.model.ValidationProfile;

import java.io.Serializable;
import java.util.List;


/**
 * Contains the result of a validation. This is the top class of validation result.
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
  private final List<ValidationResultElement> results;

  private final List<ChecklistValidationResult> checklists;

  /**
   * Use {@link ValidationResultBuilders} to get new instances.
   *
   * @param indexeable
   * @param fileFormat
   * @param validationProfile
   * @param errorCode
   */
  ValidationResult(Boolean indexeable, String fileName, FileFormat fileFormat, ValidationProfile validationProfile,
                   List<ValidationResultElement> results, ValidationErrorCode errorCode, List<ChecklistValidationResult> checklists) {
    this.indexeable = indexeable;
    this.fileName = fileName;
    this.fileFormat = fileFormat;
    this.validationProfile = validationProfile;
    this.results = results;
    this.errorCode = errorCode;
    this.checklists = checklists;
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

  public List<ChecklistValidationResult> getChecklists() {
    return checklists;
  }
}
