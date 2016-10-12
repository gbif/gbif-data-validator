package org.gbif.occurrence.validation.evaluator;

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
 * OccurrenceInterpretationEvaluator unit test
 */
public class OccurrenceInterpretationEvaluatorTest {
  
  @Test
  public void testToVerbatimOccurrence(){

    //test expected data
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator eval = new OccurrenceInterpretationEvaluator(Mockito.mock(OccurrenceInterpreter.class),
            columnMapping);

    String[] record = new String[]{"1", "2000-01-01", "2000-01-02"};
    VerbatimOccurrence occ = eval.toVerbatimOccurrence(record);

    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));

    //test record with less data than declared columns
    record = new String[]{"1", "2000-01-01"};
    occ = eval.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertNull(occ.getVerbatimField(DcTerm.modified));

    //test record with more data than declared columns
    record = new String[]{"1", "2000-01-01", "2000-01-02", "xyz"};
    occ = eval.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
  }
}
