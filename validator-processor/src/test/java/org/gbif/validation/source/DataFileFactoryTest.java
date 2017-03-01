package org.gbif.validation.source;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DataFileFactory}
 */
public class DataFileFactoryTest {

  private static final String TEST_DWC_FILE_LOCATION = "dwca/dwca-taxon";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testPrepareSourceDwc() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "dwca-taxon", FileFormat.DWCA, "");

    DwcDataFile preparedDwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDwcDataFile.getTabularDataFiles().size());

    //all components should points to the parent DataFile
    preparedDwcDataFile.getTabularDataFiles()
            .forEach( df -> assertEquals(dataFile, df.getParent().get()));

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDwcDataFile.getCore().getFilePath().toString(), ".txt"));
  }

  @Test
  public void testDetermineRecordIdentifier() {
    Optional<TermIndex> id = DataFileFactory.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLatitude, DwcTerm.occurrenceID));
    assertEquals(DwcTerm.occurrenceID, id.get().getTerm());
    assertEquals(1, id.get().getIndex());

    id = DataFileFactory.determineRecordIdentifier(Arrays.asList(DwcTerm.taxonID, DwcTerm.scientificName));
    assertEquals(DwcTerm.taxonID, id.get().getTerm());

    //eventId should be picked even if taxonID is there
    id = DataFileFactory.determineRecordIdentifier(Arrays.asList(DwcTerm.eventID, DwcTerm.scientificName, DwcTerm.taxonID));
    assertEquals(DwcTerm.eventID, id.get().getTerm());

    id = DataFileFactory.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLongitude, DwcTerm.scientificName,
            DcTerm.identifier));
    assertEquals(DcTerm.identifier, id.get().getTerm());

    //eventId should be picked even if taxonID is there
    id = DataFileFactory.determineRecordIdentifier(Arrays.asList(DwcTerm.decimalLongitude, DwcTerm.scientificName, DwcTerm.decimalLatitude));
    assertFalse(id.isPresent());
  }
}
