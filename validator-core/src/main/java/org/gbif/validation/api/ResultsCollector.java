package org.gbif.validation.api;

import org.gbif.validation.api.model.RecordEvaluationResult;

/**
 * Interface defining an {@link RecordEvaluationResult} collector.
 */
public interface ResultsCollector {

  void collect(RecordEvaluationResult result);

}
