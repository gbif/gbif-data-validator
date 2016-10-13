package org.gbif.occurrence.validation.evaluator;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.model.EvaluationType;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * RecordValueEvaluator unit tests
 */
public class RecordValueEvaluatorTest {

  @Test
  public void testEvaluate(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    RecordValueEvaluator mandatoryEventDateEvaluator = new RecordValueEvaluator(EvaluationType.MANDATORY_VALUE_NOT_PROVIDED,
            StringUtils::isNotBlank, DwcTerm.eventDate, 1);

    String[] record = new String[]{"1", null, "2000-01-01"};
    RecordEvaluationResult result = mandatoryEventDateEvaluator.evaluate(1l, record);
    assertNotNull(result);
    assertNotNull(result.getDetails());
    assertEquals(EvaluationType.MANDATORY_VALUE_NOT_PROVIDED, result.getDetails().get(0).getEvaluationType());

    record = new String[]{"1", "2000-01-01", "2000-01-01"};
    assertNull(mandatoryEventDateEvaluator.evaluate(1l, record));
  }
}
