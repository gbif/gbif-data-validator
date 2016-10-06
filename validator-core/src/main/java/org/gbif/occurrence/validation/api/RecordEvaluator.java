package org.gbif.occurrence.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.model.EvaluationResult;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * C.G. I would rename this class RecordEvaluator
 */
public interface RecordEvaluator<T extends EvaluationResult> {

  /**
   *
   * @param id identifier for the record within the context
   * @param record
   * @return
   */
  T process(@Nullable String id, Map<Term, String> record);

  String[] getFields();

}
