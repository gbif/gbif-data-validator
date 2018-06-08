package org.gbif.validation.source;

import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

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
  private static final String TEST_BROKEN_META_FILE_LOCATION = "dwca/dwca-meta-broken";
  private static final String TEST_EMPTY_CSV_FILE_LOCATION = "tabular/empty.csv";
  private static final String TEST_EMPTY_XLSX_FILE_LOCATION = "workbooks/empty.xlsx";

  private static final String TEST_OCC_XLSX_FILE_LOCATION = "workbooks/occurrence-workbook.xlsx";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testPrepareDataFile() throws IOException, UnsupportedDataFileException {
    DataFile dataFile = TestUtils.getDwcaDataFile(TEST_DWC_FILE_LOCATION, "dwca-taxon");

    DwcDataFile preparedDwcDataFile = prepareDataFile(dataFile, folder.newFolder().toPath());
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDwcDataFile.getTabularDataFiles().size());

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDwcDataFile.getCore().getFilePath().toString(), ".txt"));
  }

  /**
   * Test an archive with a meta.xml that points to a non-existing file.
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test (expected = UnsupportedArchiveException.class)
  public void testPrepareDataFileBrokenMeta() throws IOException, UnsupportedDataFileException {
    DataFile dataFile = TestUtils.getDwcaDataFile(TEST_BROKEN_META_FILE_LOCATION, "dwca-broken-meta");
    DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
  }

  @Test
  public void testXLSXFile() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_OCC_XLSX_FILE_LOCATION);
    DataFile dataFile = new DataFile(UUID.randomUUID(), testFile.toPath(), "my-xlsx-file.xlsx", FileFormat.SPREADSHEET,
            ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET, ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET);
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
//  @Test(expected = UnsupportedDataFileException.class)
//  public void testEmptyCSV() throws IOException, UnsupportedDataFileException {
//    File testFile = FileUtils.getClasspathFile(TEST_EMPTY_CSV_FILE_LOCATION);
//    DataFile dataFile = new DataFile(testFile.toPath(), "empty-csv", FileFormat.TABULAR, "");
//    prepareDataFile(dataFile, folder.newFolder().toPath());
//  }

  /**
   * Should throw UnsupportedDataFileException: application/vnd.ms-excel conversion returned no content (no line)
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test( expected = UnsupportedDataFileException.class)
  public void testEmptySpreadsheet() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_EMPTY_XLSX_FILE_LOCATION);
    DataFile dataFile = new DataFile(UUID.randomUUID(), testFile.toPath(), "empty-xlsx", FileFormat.SPREADSHEET,
            ExtraMediaTypes.APPLICATION_EXCEL, ExtraMediaTypes.APPLICATION_EXCEL);
    prepareDataFile(dataFile, folder.newFolder().toPath());
  }

}
