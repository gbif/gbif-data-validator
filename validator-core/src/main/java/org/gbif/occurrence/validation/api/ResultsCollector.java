package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.List;

public interface ResultsCollector<R> {

  void accumulate(RecordStructureEvaluationResult result);
  void accumulate(RecordInterpretionBasedEvaluationResult result);

  List<RecordStructureEvaluationResult> getRecordStructureEvaluationResult();
  R getAggregatedResult();
}
