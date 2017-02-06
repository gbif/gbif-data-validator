package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationType;

import org.junit.Test;

import static org.gbif.validation.api.result.ValidationIssues.withRelatedData;

import static org.junit.Assert.assertNotNull;

/**
 * Unit tests related to {@link ValidationIssues}, factory of {@link ValidationIssue} instances.
 */
public class ValidationIssuesTest {

  @Test(expected = IllegalStateException.class)
  public void testIllegalWithRelatedDataUsage() {
    withRelatedData(EvaluationType.COORDINATE_INVALID, "-190 is out of bound");
  }

  @Test
  public void testWithRelatedDataUsage() {
    assertNotNull(ValidationIssues.withRelatedData(EvaluationType.UNKNOWN_TERM, "decLat"));
  }

}
