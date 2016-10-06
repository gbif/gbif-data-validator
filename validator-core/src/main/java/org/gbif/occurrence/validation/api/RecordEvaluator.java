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
   * @param lineNumber number of the line within the context, can be null
   * @param record
   * @return
   */
  RecordEvaluationResult process(@Nullable Long lineNumber, Map<Term, String> record);

  String[] getFields();

}
