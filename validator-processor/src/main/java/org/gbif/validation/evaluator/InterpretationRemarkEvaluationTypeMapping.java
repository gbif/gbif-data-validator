package org.gbif.validation.evaluator;

import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.validation.api.model.EvaluationType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Basically a 1-to-1 mapping between {@link EvaluationType} and supported {@link OccurrenceIssue} and
 * {@link NameUsageIssue}.
 *
 */
public class InterpretationRemarkEvaluationTypeMapping {

  /**
   * Utility classes hide constructors.
   */
  private InterpretationRemarkEvaluationTypeMapping(){
    //empty constructor
  }

  //we do not support deprecated OccurrenceIssue
  public static final List<InterpretationRemark> UNSUPPORTED_ISSUES = Collections.unmodifiableList(Arrays.asList(
          OccurrenceIssue.COORDINATE_ACCURACY_INVALID,
          OccurrenceIssue.COORDINATE_PRECISION_UNCERTAINTY_MISMATCH,
          NameUsageIssue.BACKBONE_MATCH_FUZZY,
          /** currently not supported by the validator */
          NameUsageIssue.BACKBONE_MATCH_NONE)
  );

  public static final Map<InterpretationRemark, EvaluationType> OCCURRENCE_ISSUE_MAPPING;

  static {

    List<Enum<? extends InterpretationRemark>> allRemarks = new ArrayList<>();
    allRemarks.addAll(Arrays.asList(OccurrenceIssue.values()));
    allRemarks.addAll(Arrays.asList(NameUsageIssue.values()));

    Map<InterpretationRemark, EvaluationType> mapping =
            allRemarks
                    .stream()
                    .filter(x -> !UNSUPPORTED_ISSUES.contains(x))
                    .collect(Collectors.toMap(k-> (InterpretationRemark)k, v -> EvaluationType.valueOf(v.name())));

    OCCURRENCE_ISSUE_MAPPING = Collections.unmodifiableMap(mapping);
  }

}
