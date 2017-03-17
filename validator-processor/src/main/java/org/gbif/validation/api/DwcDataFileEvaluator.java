package org.gbif.validation.api;

import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Optional;

/**
 * The {@link DwcDataFileEvaluator} is similar to {@link ResourceStructureEvaluator} but is expected
 * to run at a different time (after the structure has been validated).
 */
public interface DwcDataFileEvaluator {
  Optional<List<ValidationResultElement>> evaluate(DwcDataFile dwcDataFile);
}
