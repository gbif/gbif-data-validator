package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DataFileFactory}
 */
public class DataFileFactoryTest {

  private static final String TEST_DWC_FILE_LOCATION = "dwca/dwca-taxon";

  @Test
  public void testPrepareSourceDwc() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setFilePath(testFile.toPath());

    List<DataFile> preparedDataFiles = DataFileFactory.prepareDataFile(dataFile);
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDataFiles.size());

    //all components should points to the parent DataFile
    preparedDataFiles.forEach( df -> assertEquals(dataFile, df.getParent().get()));

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDataFiles.get(0).getFilePath().toString(), ".txt"));
  }
}
