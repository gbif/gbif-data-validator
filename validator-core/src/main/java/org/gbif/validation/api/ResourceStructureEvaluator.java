package org.gbif.validation.api;

import org.gbif.validation.api.result.ValidationResultElement;

import java.nio.file.Path;
import java.util.Optional;

/**
 * {@link RecordEvaluator} is responsible to take a resource and produce a {@link ValidationResultElement} after
 * evaluation the structure of the resource.
 */
public interface ResourceStructureEvaluator {

  /**
   *
   * @param dwcFolder where the resource is located
   * @param sourceFilename the file name submitted by the user
   * @return
   */
  Optional<ValidationResultElement> evaluate(Path dwcFolder, String sourceFilename);
}
