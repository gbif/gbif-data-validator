package org.gbif.validation.collector;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Basic unit tests for {@link TermsFrequencyCollector}.
 */
public class TermsFrequencyCollectorTest {

  @Test
  public void baseTest() {
    List<Term> columnHeaders = Arrays.asList(new Term[]{DwcTerm.occurrenceID, DwcTerm.eventDate, DwcTerm.scientificName});
    testTermsFrequencyCollector(new TermsFrequencyCollector(columnHeaders, false));
    //just make sure the useConcurrentMap works even if this test do no test concurrency
    testTermsFrequencyCollector(new TermsFrequencyCollector(columnHeaders, true));
  }

  /**
   * Test a single TermsFrequencyCollector instance.
   * @param tfc
   */
  private void testTermsFrequencyCollector(TermsFrequencyCollector tfc) {
    tfc.collect(new String[]{"1", "2000-01-01", "Gulo gulo"});
    tfc.collect(new String[]{"2", "2000-01-01", ""});
    tfc.collect(new String[]{"3", "2000-01-01", " "});
    tfc.collect(new String[]{"4", null, "\t"});

    assertEquals(tfc.getTermFrequency().get(DwcTerm.occurrenceID).intValue(), 4);
    assertEquals(tfc.getTermFrequency().get(DwcTerm.eventDate).intValue(), 3);
    assertEquals(tfc.getTermFrequency().get(DwcTerm.scientificName).intValue(), 1);
  }
}
