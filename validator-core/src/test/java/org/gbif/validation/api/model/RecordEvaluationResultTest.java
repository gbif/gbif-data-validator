package org.gbif.validation.api.model;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests related to {@link RecordEvaluationResult}.
 */
public class RecordEvaluationResultTest {

  @Test
  public void testFluentBuilder() {
    RecordEvaluationResult result = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1l)
            .addBaseDetail(EvaluationType.COLUMN_MISMATCH, "12", "11")
            .build();
    assertNotNull(result);
  }

  @Test
  public void testMerge() {
    Map<Term, String> relatedData = new HashMap<>();
    relatedData.put(DwcTerm.basisOfRecord, "n/a");
    RecordEvaluationResult result = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1l)
            .addBaseDetail(EvaluationType.COLUMN_MISMATCH, "12", "11")
            .build();
    RecordEvaluationResult result2 = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1l)
            .addInterpretationDetail(EvaluationType.BASIS_OF_RECORD_INVALID, relatedData)
            .build();

    RecordEvaluationResult mergedResult = RecordEvaluationResult.Builder.merge(result, result2);
    assertNotNull(mergedResult);
    assertNotNull(mergedResult.getDetails());

    assertEquals(2, mergedResult.getDetails().size());
  }
}
