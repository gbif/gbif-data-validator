package org.gbif.validation.evaluator.runner;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RowTypeKey;

/**
 * Functional interface to define how to run a {@link RecordCollectionEvaluator}
 */
@FunctionalInterface
public interface RecordCollectionEvaluatorRunner {
  void run(DwcDataFile dataFile, RowTypeKey rowTypeKey, RecordCollectionEvaluator recordCollectionEvaluator);
}
