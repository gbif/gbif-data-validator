package org.gbif.validation.evaluator;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.validation.api.model.EvaluationType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Basically a 1-to-1 mapping between {@link EvaluationType} and supported {@link OccurrenceIssue}.
 *
 */
public class OccurrenceIssueEvaluationTypeMapping {

  /**
   * Utility classes hide constructors.
   */
  private OccurrenceIssueEvaluationTypeMapping(){
    //empty constructor
  }
  //we do not support deprecated OccurrenceIssue
  public static final List<OccurrenceIssue> UNSUPPORTED_OCCURRENCE_ISSUES = Collections.unmodifiableList(Arrays.asList(
          OccurrenceIssue.COORDINATE_ACCURACY_INVALID,
          OccurrenceIssue.COORDINATE_PRECISION_UNCERTAINTY_MISMATCH));

  public static final Map<OccurrenceIssue, EvaluationType> OCCURRENCE_ISSUE_MAPPING;

  static {
    Map<OccurrenceIssue, EvaluationType> mapping =
            Arrays.asList(OccurrenceIssue.values())
                    .stream()
                    .filter(x -> !UNSUPPORTED_OCCURRENCE_ISSUES.contains(x))
                    .collect(Collectors.toMap(Function.identity(), v -> EvaluationType.valueOf(v.name())));
    OCCURRENCE_ISSUE_MAPPING = Collections.unmodifiableMap(mapping);
  }

}
