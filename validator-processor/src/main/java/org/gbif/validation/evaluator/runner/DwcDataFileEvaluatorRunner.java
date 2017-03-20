package org.gbif.validation.evaluator.runner;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;

/**
 * Functional interface to define how to run a {@link DwcDataFileEvaluator}
 */
@FunctionalInterface
public interface DwcDataFileEvaluatorRunner {
  void run(DwcDataFile dataFile, DwcDataFileEvaluator metadataEvaluator);
}
