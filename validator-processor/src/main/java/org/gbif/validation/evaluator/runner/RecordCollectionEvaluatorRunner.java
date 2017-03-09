package org.gbif.validation.evaluator.runner;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;

/**
 * Functional interface to define how to run a {@link RecordCollectionEvaluator}
 */
@FunctionalInterface
public interface RecordCollectionEvaluatorRunner {
  void run(DwcDataFile dataFile, Term rowType, RecordCollectionEvaluator recordCollectionEvaluator);
}
