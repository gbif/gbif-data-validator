package org.gbif.validation.evaluator;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Optional;

/**
 * Ensure all the component we expect from {@link DwcDataFile} are available.
 */
public class PrerequisiteEvaluator implements DwcDataFileEvaluator {

  @Override
  public Optional<List<ValidationResultElement>> evaluate(DwcDataFile dwcDataFile) {
    return null;
  }
}
