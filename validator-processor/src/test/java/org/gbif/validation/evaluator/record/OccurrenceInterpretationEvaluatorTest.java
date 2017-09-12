package org.gbif.validation.evaluator.record;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * OccurrenceInterpretationEvaluator unit tests
 */
public class OccurrenceInterpretationEvaluatorTest {

  private static final Map<Term, String> DEFAULT_VALUES = new HashMap<>();
  static {
    DEFAULT_VALUES.put(DwcTerm.basisOfRecord, BasisOfRecord.FOSSIL_SPECIMEN.name());
  }
  private static final Term[] COLUMN_MAPPING = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
  private static final TermIndex OCC_ID_TERM_INDEX = new TermIndex(0, DwcTerm.occurrenceID);

  @Test
  public void testToVerbatimOccurrence(){

    //test expected data
    OccurrenceInterpretationEvaluator evaluator = createInterpreter(COLUMN_MAPPING, OCC_ID_TERM_INDEX);
    List<String> record = Arrays.asList("1", "2000-01-01", "2000-01-02");
    VerbatimOccurrence occ = evaluator.toVerbatimOccurrence(record);

    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    //test record with less data than declared columns
    record = Arrays.asList("1", "2000-01-01");
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertNull(occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    //test record with more data than declared columns
    record = Arrays.asList("1", "2000-01-01", "2000-01-02", "xyz");
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals("1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals("2000-01-01", occ.getVerbatimField(DwcTerm.eventDate));
    assertEquals("2000-01-02", occ.getVerbatimField(DcTerm.modified));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));
  }

  @Test
  public void testEvaluateWithNulls(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator evaluator = new OccurrenceInterpretationEvaluator(mock(OccurrenceInterpreter.class),
            columnMapping, null, OCC_ID_TERM_INDEX);
    assertNull(evaluator.evaluate(null, null));
  }

  @Test
  public void testEvaluate(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified};
    OccurrenceInterpretationEvaluator evaluator = createInterpreter(columnMapping, OCC_ID_TERM_INDEX);

    List<String> record = Arrays.asList("1-18-ABB", "2000-01-01", "2000-01-02");
    RecordEvaluationResult result = evaluator.evaluate(1L, record);
    assertEquals("1-18-ABB", result.getRecordId());
  }

  @Test
  public void testEvaluateWithDefaultValues(){
    Term[] columnMapping = new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DcTerm.modified, DwcTerm.basisOfRecord};
    OccurrenceInterpretationEvaluator evaluator = createInterpreter(columnMapping, OCC_ID_TERM_INDEX);

    //ensure the specified value is provided
    List<String> record = Arrays.asList("1", "2000-01-01", "2000-01-02", BasisOfRecord.LIVING_SPECIMEN.name());
    VerbatimOccurrence occ = evaluator.toVerbatimOccurrence(record);
    assertEquals(BasisOfRecord.LIVING_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));

    record = Arrays.asList("2", "2000-01-01", "2000-01-02", "");
    occ = evaluator.toVerbatimOccurrence(record);
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN.name(), occ.getVerbatimField(DwcTerm.basisOfRecord));
  }

  /**
   * The mock OccurrenceInterpreter will take a an occurrence and return it in OccurrenceInterpretationResult.
   * @param columnMapping
   * @param recordIdentifier
   * @return
   */
  private OccurrenceInterpretationEvaluator createInterpreter(Term[] columnMapping, TermIndex recordIdentifier) {
    OccurrenceInterpreter occurrenceInterpreter = Mockito.mock(OccurrenceInterpreter.class);
    when(occurrenceInterpreter.interpret(any(), any()))
            .thenAnswer( i -> new OccurrenceInterpretationResult(new Occurrence(i.getArgument(0)),
                    new Occurrence(i.getArgument(0))));

    return new OccurrenceInterpretationEvaluator(occurrenceInterpreter,
            columnMapping, DEFAULT_VALUES, recordIdentifier);
  }

}
