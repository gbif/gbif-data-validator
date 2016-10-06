package org.gbif.occurrence.validation.evaluator;

import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Safeguard tests to make sure OCCURRENCE_ISSUE_MAPPING is in sync with OccurrenceIssue.
 *
 */
public class OccurrenceIssueEvaluationTypeMappingTest {

  private List<OccurrenceIssue> deprecatedOccurrenceIssues = Arrays.asList(
          OccurrenceIssue.COORDINATE_ACCURACY_INVALID,
          OccurrenceIssue.COORDINATE_PRECISION_UNCERTAINTY_MISMATCH);

  @Test
  public void testOccurrenceIssueEvaluationTypeMapping() {

    List<OccurrenceIssue> allOccurrenceIssue = new ArrayList<>(Arrays.asList(OccurrenceIssue.values()));
    allOccurrenceIssue.removeAll(deprecatedOccurrenceIssues);

    for(OccurrenceIssue occIssue : allOccurrenceIssue) {
      assertTrue(OccurrenceIssueEvaluationTypeMapping.OCCURRENCE_ISSUE_MAPPING.containsKey(occIssue));
    }
  }

}
