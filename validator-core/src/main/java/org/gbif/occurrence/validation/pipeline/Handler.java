package org.gbif.occurrence.validation.pipeline;

import org.gbif.occurrence.validation.model.EvaluationResult;

public interface Handler<R extends EvaluationResult,T> {

  R handle(DataRecord<T> record);

}
