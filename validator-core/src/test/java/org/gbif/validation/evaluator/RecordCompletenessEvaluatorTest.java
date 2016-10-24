package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * RecordCompletenessEvaluator unit tests
 */
public class RecordCompletenessEvaluatorTest {

  @Test
  public void testEvaluate(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    RecordCompletenessEvaluator mandatoryEventDateEvaluator = new RecordCompletenessEvaluator(
            EvaluationType.TEMPORAL_DATA_NOT_PROVIDED, new Term[]{DwcTerm.eventDate}, new Integer[]{1});

    String[] record = new String[]{"1", null, "2000-01-01"};
    RecordEvaluationResult result = mandatoryEventDateEvaluator.evaluate(1l, record);
    assertNotNull(result);
    assertNotNull(result.getDetails());
    assertEquals(EvaluationType.TEMPORAL_DATA_NOT_PROVIDED, result.getDetails().get(0).getEvaluationType());

    record = new String[]{"1", "2000-01-01", "2000-01-01"};
    assertNull(mandatoryEventDateEvaluator.evaluate(1l, record));
  }
}
