package org.gbif.validation.evaluator;

import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Class defining the "rule" to determine if a resource can be indexed or not.
 * TODO when (if?) needed, add the mapping between the ValidationProfile and the rule
 */
public class IndexableRules {

  /**
   * {@link EvaluationType} that makes the resource non-indexable.
   */
  private static final Set<EvaluationType> NON_INDEXABLE_EVALUATION_TYPE = ImmutableSet.of(
          EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED,
          EvaluationType.DUPLICATED_TERM,
          EvaluationType.LICENSE_MISSING_OR_UNKNOWN);

  /**
   * Static utility class
   */
  private IndexableRules() {}

  /**
   * Return {@link EvaluationType} that make a resource non-indexable.
   * @return
   */
  public static Set<EvaluationType> getNonIndexableEvaluationType() {
    return NON_INDEXABLE_EVALUATION_TYPE;
  }

  /**
   * Given a list of {@link ValidationResultElement}, determine if the resource is indexable or not.
   *
   * @param resultElements
   * @return
   */
  public static boolean isIndexable(List<ValidationResultElement> resultElements) {
    for(ValidationResultElement vre : resultElements) {
      if (vre.containsAny(NON_INDEXABLE_EVALUATION_TYPE)) {
        return false;
      }
    }
    return true;
  }
}
