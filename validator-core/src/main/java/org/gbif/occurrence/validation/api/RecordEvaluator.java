package org.gbif.occurrence.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Evaluator is responsible to take a record and produce an {@link RecordEvaluationResult}.
 */
public interface RecordEvaluator {

  /**
   *
   * @param id identifier for the record within the context
   * @param record
   * @return
   */
  RecordEvaluationResult process(@Nullable String id, Map<Term, String> record);

  String[] getFields();

}
