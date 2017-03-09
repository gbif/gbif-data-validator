package org.gbif.validation.api;

import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Optional;

/**
 * The {@link MetadataEvaluator} contains the same method as {@link ResourceStructureEvaluator} but is expected
 * to run at a different time (after the structure has been validated).
 */
public interface MetadataEvaluator {
  Optional<List<ValidationResultElement>> evaluate(DwcDataFile dwcDataFile);
}
