package org.gbif.validation.api;

import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;

/**
 * {@link RecordEvaluator} is responsible to take a resource and produce a {@link ValidationResultElement} after
 * evaluation the structure of the resource.
 */
public interface ResourceStructureEvaluator {

  /**
   * Evaluate a {@link DataFile} and optionally produce a {@link ValidationResultElement}.
   *
   * @param dataFile where the resource is located
   * @return list of all {@link ValidationResultElement} or Optional.empty() if nothing to report
   */
  Optional<List<ValidationResultElement>> evaluate(@NotNull DataFile dataFile);
}
