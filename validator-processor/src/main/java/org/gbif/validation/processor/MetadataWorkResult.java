package org.gbif.validation.processor;

import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Optional;

/**
 * This class encapsulates the result of processing a metadata file by an Akka actor.
 */
class MetadataWorkResult {

  private DataWorkResult.Result result;
  private final List<ValidationResultElement> validationResultElements;

  public MetadataWorkResult(DataWorkResult.Result result, List<ValidationResultElement> validationResultElements) {
    this.result = result;
    this.validationResultElements = validationResultElements;
  }

  public DataWorkResult.Result getResult() {
    return result;
  }

  public Optional<List<ValidationResultElement>> getValidationResultElements() {
    return Optional.ofNullable(this.validationResultElements);
  }

}
