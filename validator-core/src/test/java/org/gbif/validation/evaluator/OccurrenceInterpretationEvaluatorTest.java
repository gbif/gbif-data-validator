package org.gbif.validation.evaluator;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;

import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * OccurrenceInterpretationEvaluator unit tests
 */
public class OccurrenceInterpretationEvaluatorTest {

  @Test
  public void testToVerbatimOccurrence(){

    //test expected data
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator evaluator = new OccurrenceInterpretationEvaluator(Mockito.mock(OccurrenceInterpreter.class),
            columnMapping);

    String[] record = new String[]{"1", "2000-01-01", "2000-01-02"};
    VerbatimOccurrence occ = evaluator.toVerbatimOccurrence(record);

    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));

    //test record with less data than declared columns
    record = new String[]{"1", "2000-01-01"};
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertNull(occ.getVerbatimField(DcTerm.modified));

    //test record with more data than declared columns
    record = new String[]{"1", "2000-01-01", "2000-01-02", "xyz"};
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
  }

  @Test
  public void testEvaluate(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator evaluator = new OccurrenceInterpretationEvaluator(Mockito.mock(OccurrenceInterpreter.class),
            columnMapping);
    assertNull(evaluator.evaluate(null, null));
  }
}
