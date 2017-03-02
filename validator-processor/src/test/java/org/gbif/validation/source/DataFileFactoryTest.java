package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
  public void testPrepareSourceDwc() throws IOException, UnsupportedDataFileException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "dwca-taxon", FileFormat.DWCA, "");

    DwcDataFile preparedDwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDwcDataFile.getTabularDataFiles().size());

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDwcDataFile.getCore().getFilePath().toString(), ".txt"));
  }

}
