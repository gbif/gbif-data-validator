package org.gbif.validation.api.result;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.validation.api.TermWithinRowType;
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
    withRelatedData(EvaluationType.COORDINATE_INVALID, TermWithinRowType.ofRowType(DwcTerm.Occurrence));
  }

  @Test
  public void testWithRelatedDataUsage() {
    assertNotNull(ValidationIssues.withRelatedData(EvaluationType.UNKNOWN_TERM, TermWithinRowType.of(
            DwcTerm.Occurrence, TermFactory.instance().findTerm("decLat"))));
  }

}
