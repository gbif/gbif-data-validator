package org.gbif.validation.evaluator.record;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * OccurrenceInterpretationEvaluator unit tests
 */
public class OccurrenceInterpretationEvaluatorTest {

  private static final Map<Term, String> DEFAULT_VALUES = new HashMap<>();
  static {
    DEFAULT_VALUES.put(DwcTerm.basisOfRecord, BasisOfRecord.FOSSIL_SPECIMEN.name());
  }
  Term[] COLUMN_MAPPING = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};

  @Test
  public void testToVerbatimOccurrence(){


    //test expected data
    OccurrenceInterpretationEvaluator evaluator = createInterpreter(COLUMN_MAPPING);
    String[] record = {"1", "2000-01-01", "2000-01-02"};
    VerbatimOccurrence occ = evaluator.toVerbatimOccurrence(record);

    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    //test record with less data than declared columns
    record = new String[]{"1", "2000-01-01"};
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertNull(occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    //test record with more data than declared columns
    record = new String[]{"1", "2000-01-01", "2000-01-02", "xyz"};
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));
  }

  @Test
  public void testEvaluateWithNulls(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator evaluator = new OccurrenceInterpretationEvaluator(Mockito.mock(OccurrenceInterpreter.class),
            DwcTerm.Occurrence, columnMapping, Optional.empty());
    assertNull(evaluator.evaluate(null, null));
  }

  @Test
  public void testEvaluateWithDefaultValues(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified, DwcTerm.basisOfRecord};
    OccurrenceInterpretationEvaluator evaluator = createInterpreter(columnMapping);

    //ensure the specified value is provided
    String[] record = {"1", "2000-01-01", "2000-01-02", BasisOfRecord.LIVING_SPECIMEN.name()};
    VerbatimOccurrence occ = evaluator.toVerbatimOccurrence(record);
    assertEquals(BasisOfRecord.LIVING_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    record = new String[]{"2", "2000-01-01", "2000-01-02", ""};
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));
  }

  private OccurrenceInterpretationEvaluator createInterpreter(Term[] columnMapping) {
    //test expected data
    return new OccurrenceInterpretationEvaluator(Mockito.mock(OccurrenceInterpreter.class),
            DwcTerm.Occurrence, columnMapping, Optional.of(DEFAULT_VALUES));
  }
}
