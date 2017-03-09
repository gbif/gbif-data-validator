package org.gbif.validation.evaluator.runner;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;

import java.util.List;

/**
 * Functional interface to define how to run a {@link RecordEvaluator}
 */
@FunctionalInterface
public interface RecordEvaluatorRunner {
  void run(List<TabularDataFile> dataFiles, Term rowType, RecordEvaluator recordEvaluator);
}
