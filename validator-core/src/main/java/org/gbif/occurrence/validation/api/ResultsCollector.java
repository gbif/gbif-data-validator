package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.model.RecordEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.List;

/**
 * Interface defining the collector of results.
 * @param <R>
 */
public interface ResultsCollector<R> {

  void accumulate(RecordEvaluationResult result);

  List<RecordStructureEvaluationResult> getRecordStructureEvaluationResult();

  R getAggregatedResult();
}
