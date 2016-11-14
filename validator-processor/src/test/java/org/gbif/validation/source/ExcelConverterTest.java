package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;

/**
 * {@link ExcelConverter} related tests
 */
public class ExcelConverterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final String CSV_TEST_FILE_LOCATION = "workbooks/occurrence-workbook.csv";
  private static final String XLSX_TEST_FILE_LOCATION = "workbooks/occurrence-workbook.xlsx";
  private static final String XLS_TEST_FILE_LOCATION = "workbooks/occurrence-workbook.xls";

  @Test
  public void testDwcReader() throws IOException, InvalidFormatException {
    testDwcReader(FileUtils.getClasspathFile(XLSX_TEST_FILE_LOCATION));
    testDwcReader(FileUtils.getClasspathFile(XLS_TEST_FILE_LOCATION));
  }

  public void testDwcReader(File workbookFile) throws IOException, InvalidFormatException {

    File testCsvFile = FileUtils.getClasspathFile(CSV_TEST_FILE_LOCATION);
    File testFile = folder.newFile();
    SpreadsheetConverters.convertExcelToCSV(workbookFile.toPath(), testFile.toPath());

    assertTrue(org.apache.commons.io.FileUtils.contentEqualsIgnoreEOL(testFile, testCsvFile, "UTF-8"));
  }

}
