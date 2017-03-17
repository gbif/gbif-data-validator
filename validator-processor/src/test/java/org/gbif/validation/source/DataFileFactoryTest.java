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

import static org.gbif.validation.source.DataFileFactory.prepareDataFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DataFileFactory}
 */
public class DataFileFactoryTest {

  private static final String TEST_DWC_FILE_LOCATION = "dwca/dwca-taxon";
  private static final String TEST_EMPTY_CSV_FILE_LOCATION = "tabular/empty.csv";
  private static final String TEST_NO_ID_CSV_FILE_LOCATION = "tabular/no_id.csv";
  private static final String TEST_EMPTY_XLSX_FILE_LOCATION = "workbooks/empty.xlsx";

  private static final String TEST_OCC_XLSX_FILE_LOCATION = "workbooks/occurrence-workbook.xlsx";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testPrepareDataFile() throws IOException, UnsupportedDataFileException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "dwca-taxon", FileFormat.DWCA, "");

    DwcDataFile preparedDwcDataFile = prepareDataFile(dataFile, folder.newFolder().toPath());
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDwcDataFile.getTabularDataFiles().size());

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDwcDataFile.getCore().getFilePath().toString(), ".txt"));
  }

  @Test
  public void testXLSXFile() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_OCC_XLSX_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "my-xlsx-file.xlsx", FileFormat.SPREADSHEET,
            ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET);
    DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
    //ensure we got a tabularDataFile
    assertEquals(1, dwcDataFile.getTabularDataFiles().size());

    //ensure the name of the original file is preserved
    assertEquals("my-xlsx-file.xlsx", dwcDataFile.getTabularDataFiles().get(0).getSourceFileName());
  }


  /**
   * Should throw UnsupportedDataFileException: Unable to detect field delimiter
   *
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test(expected = UnsupportedDataFileException.class)
  public void testEmptyCSV() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_EMPTY_CSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "empty-csv", FileFormat.TABULAR, "");
    prepareDataFile(dataFile, folder.newFolder().toPath());
  }

  /**
   * Should throw UnsupportedDataFileException: Unable to detect field delimiter
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test( expected = UnsupportedDataFileException.class)
  public void testCSVnoRecordId() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_NO_ID_CSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "no-id", FileFormat.TABULAR, "");
    prepareDataFile(dataFile, folder.newFolder().toPath());
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
    prepareDataFile(dataFile, folder.newFolder().toPath());
  }

}
