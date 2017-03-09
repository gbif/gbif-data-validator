package org.gbif.validation.evaluator.runner;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.MetadataEvaluator;

/**
 * Functional interface to define how to run a {@link MetadataEvaluator}
 */
@FunctionalInterface
public interface MetadataEvaluatorRunner {
  void run(DwcDataFile dataFile, MetadataEvaluator metadataEvaluator);
}
