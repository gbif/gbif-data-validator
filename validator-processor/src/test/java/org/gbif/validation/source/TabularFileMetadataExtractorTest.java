package org.gbif.validation.source;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.TermIndex;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * Test related to {@link TabularFileMetadataExtractor}
 */
public class TabularFileMetadataExtractorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDetermineRowType() {
    Optional<Term> rowType = TabularFileMetadataExtractor
            .determineRowType(Arrays.asList(DwcTerm.decimalLatitude, DwcTerm.occurrenceID));
    assertEquals(DwcTerm.Occurrence, rowType.get());
  }

  @Test
  public void testDetermineRecordIdentifier() {
    Optional<TermIndex> id = TabularFileMetadataExtractor.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLatitude, DwcTerm.occurrenceID));
    assertEquals(DwcTerm.occurrenceID, id.get().getTerm());
    assertEquals(1, id.get().getIndex());

    id = TabularFileMetadataExtractor.determineRecordIdentifier(Arrays.asList(DwcTerm.taxonID, DwcTerm.scientificName));
    assertEquals(DwcTerm.taxonID, id.get().getTerm());

    //eventId should be picked even if taxonID is there
    id = TabularFileMetadataExtractor.determineRecordIdentifier(Arrays.asList(DwcTerm.eventID, DwcTerm.scientificName, DwcTerm.taxonID));
    assertEquals(DwcTerm.eventID, id.get().getTerm());

    id = TabularFileMetadataExtractor.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLongitude, DwcTerm.scientificName,
            DcTerm.identifier));
    assertEquals(DcTerm.identifier, id.get().getTerm());

    //eventId should be picked even if taxonID is there
    id = TabularFileMetadataExtractor.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLongitude, DwcTerm.scientificName, DwcTerm.decimalLatitude));
    assertFalse(id.isPresent());
  }
}
