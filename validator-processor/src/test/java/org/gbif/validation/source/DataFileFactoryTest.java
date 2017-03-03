package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.ws.util.ExtraMediaTypes;

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
  private static final String TEST_EMPTY_CSV_FILE_LOCATION = "tabular/empty.csv";
  private static final String TEST_EMPTY_XLSX_FILE_LOCATION = "workbooks/empty.xlsx";

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

  /**
   * Should throw UnsupportedDataFileException: Unable to detect field delimiter
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test( expected = UnsupportedDataFileException.class)
  public void testEmptyCSV() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_EMPTY_CSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "empty-csv", FileFormat.TABULAR, "");
    DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
  }

  /**
   * Should throw UnsupportedDataFileException: application/vnd.ms-excel conversion returned no content (no line)
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test( expected = UnsupportedDataFileException.class)
  public void testEmptySpreadsheet() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_EMPTY_XLSX_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "empty-xlsx", FileFormat.SPREADSHEET,
            ExtraMediaTypes.APPLICATION_EXCEL);
    DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
  }

}
