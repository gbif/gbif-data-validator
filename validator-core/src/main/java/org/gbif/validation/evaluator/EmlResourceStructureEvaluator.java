package org.gbif.validation.evaluator;

import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.result.ValidationResultElement;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Class to evaluate the structure of an EML file.
 * That includes xml schema validation.
 */
public class EmlResourceStructureEvaluator implements ResourceStructureEvaluator {

  @Override
  public Optional<ValidationResultElement> evaluate(Path dwcFolder, String sourceFilename) {
    //not implemented yet
    return null;
  }
}
