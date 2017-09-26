package org.gbif.validation.api.model;

import org.gbif.dwc.terms.Term;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * For each different type of evaluation result, this represents the "contract" of the details of the result.
 *
 */
public class RecordEvaluationResultDetails implements Serializable {

  private final EvaluationType evaluationType;
  private final Map<Term, String> relatedData;
  private final String expected;
  private final String found;

  public RecordEvaluationResultDetails(EvaluationType evaluationType, String expected,
                                       String found){
    this(evaluationType, expected, found, null);
  }

  public RecordEvaluationResultDetails(EvaluationType evaluationType, Map<Term, String> relatedData){
    this(evaluationType, null, null, relatedData);
  }

  /**
   * Complete constructor
   * @param evaluationType
   * @param expected
   * @param found
   * @param relatedData
   */
  private RecordEvaluationResultDetails(EvaluationType evaluationType,
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

  /**
   * Compute a key representing the input values that generated this {@link RecordEvaluationResultDetails}.
   * Useful to get the different input values that generated the same EvaluationType results.
   * @return String representing the input data, since all fields are optional the empty value is defined by "-";
   */
  public String computeInputValuesKey() {
    StringBuilder st = new StringBuilder();
    st.append(StringUtils.defaultString(found, ""))
            .append("-")
            .append(relatedData == null ? "" :
                    relatedData.entrySet().stream()
                            //sort by simpleName, we simply want a fix ordering
                            .sorted((o1, o2) -> o1.getKey().simpleName().compareTo(o2.getKey().simpleName()))
                            .map(me -> StringUtils.defaultString(me.getValue(), "null"))
                            .collect(Collectors.joining("-")));
    return st.toString();
  }

}
