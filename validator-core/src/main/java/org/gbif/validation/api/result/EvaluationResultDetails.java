package org.gbif.validation.api.result;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;
import java.util.Map;

/**
 * For each different type of evaluation result, this represents the "contract" of the details of the result.
 *
 */
public class EvaluationResultDetails implements Serializable {

  private final EvaluationType evaluationType;
  private final Map<Term, String> relatedData;
  private final String expected;
  private final String found;

  public EvaluationResultDetails(EvaluationType evaluationType, String expected,
                                 String found){
    this(evaluationType, expected, found, null);
  }

  public EvaluationResultDetails(EvaluationType evaluationType, Map<Term, String> relatedData){
    this(evaluationType, null, null, relatedData);
  }

  /**
   * Complete constructor
   * @param evaluationType
   * @param expected
   * @param found
   * @param relatedData
   */
  private EvaluationResultDetails(EvaluationType evaluationType,
                              String expected, String found, Map<Term, String> relatedData){
    this.evaluationType = evaluationType;
    this.expected = expected;
    this.found = found;
    this.relatedData = relatedData;
  }



  public String getExpected() {
    return expected;
  }

  public String getFound() {
    return found;
  }

  public Map<Term, String> getRelatedData() {
    return relatedData;
  }

  public EvaluationType getEvaluationType() {
    return evaluationType;
  }

  @Override
  public String toString() {
    return "evaluationType: " + evaluationType;
  }
}
