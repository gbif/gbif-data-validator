package org.gbif.validation.collector;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * Basic unit tests for {@link InterpretedTermsCountCollectorTest}.
 */
public class InterpretedTermsCountCollectorTest {

  @Test
  public void baseTest() {
    Term[] termToCollect = new Term[]{GbifTerm.taxonKey};
    testInterpretedTermsCountCollector(new InterpretedTermsCountCollector(Arrays.asList(termToCollect), false));
    //just make sure the useConcurrentMap works even if this test do no test concurrency
    testInterpretedTermsCountCollector(new InterpretedTermsCountCollector(Arrays.asList(termToCollect), true));
  }

  /**
   * Test a single TermsFrequencyCollector instance.
   * @param tfc
   */
  private void testInterpretedTermsCountCollector(InterpretedTermsCountCollector tfc) {
    Map<Term, Object> interpretedData = new HashMap<>();
    interpretedData.put(DwcTerm.occurrenceID, "1234");
    interpretedData.put(GbifTerm.taxonKey, 4321);

    RecordEvaluationResult.Builder bldr = new RecordEvaluationResult.Builder();
    tfc.collect(bldr.withLineNumber(1l).withInterpretedData(interpretedData).build());
    interpretedData.put(DwcTerm.eventDate, new Date());
    tfc.collect(bldr.withLineNumber(2l).withInterpretedData(interpretedData).build());
    tfc.collect(bldr.withLineNumber(3l).withInterpretedData(interpretedData).build());
    //remove GbifTerm.taxonKey for the last record
    interpretedData.remove(GbifTerm.taxonKey);
    tfc.collect(bldr.withLineNumber(4l).withInterpretedData(interpretedData).build());

    assertEquals(tfc.getInterpretedCounts().get(GbifTerm.taxonKey).intValue(), 3);
    assertNull("DwcTerm.occurrenceID is not declared as a term to collect",
            tfc.getInterpretedCounts().get(DwcTerm.occurrenceID));
  }
}
