package org.gbif.validation.source;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.RecordSource;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests related to {@link DwcReader}.
 */
public class DwcReaderTest {

  private static final String TEST_FILE_TAXON_LOCATION = "dwca/dwca-taxon";
  private static final String TEST_FILE_ID_WITH_TERM_LOCATION = "dwca/dwca-id-with-term";

  @Test
  public void testDwcReaderTaxon() {
    testDwcaReader(TEST_FILE_TAXON_LOCATION, DwcTerm.Taxon, 12, "1559060", 12);
  }

  @Test
  public void testDwcReaderIdAsTerm() {
    testDwcaReader(TEST_FILE_ID_WITH_TERM_LOCATION, DwcTerm.Occurrence, 5,
            "10d1bde8870c424ba9065de6964a269d", 11);
  }

  private void testDwcaReader(String testFile, Term expectedRowType, int expectedNumberOfHeaders,
                              String expectedLine1Column1Value, int expectedLine1Length) {
    File testFolder = FileUtils.getClasspathFile(testFile);
    try (RecordSource source = RecordSourceFactory.fromDwcA(testFolder)) {
      assertEquals(expectedRowType, ((DwcReader) source).getRowType());
      assertEquals(expectedNumberOfHeaders, source.getHeaders().length);

      String[] line1 = source.read();
      assertEquals(expectedLine1Column1Value, line1[0]);
      assertEquals(expectedLine1Length, line1.length);

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
