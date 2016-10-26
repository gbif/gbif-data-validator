package org.gbif.validation.api;

import org.gbif.validation.api.model.RecordEvaluationResult;

/**
 * Interface defining an {@link RecordEvaluationResult} collector.
 */
public interface ResultsCollector {

  int DEFAULT_MAX_NUMBER_OF_SAMPLE = 10;

  void accumulate(RecordEvaluationResult result);

}
