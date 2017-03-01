package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class OdsConverterTest {

  private static final String ODS_TEST_FILE_LOCATION = "workbooks/occurrence-workbook.ods";
  private static final String CSV_TEST_FILE_LOCATION = "workbooks/occurrence-workbook-no-empty-last-line.csv";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testOdsConverter() throws IOException, InvalidFormatException {
    testOdsConverter(FileUtils.getClasspathFile(ODS_TEST_FILE_LOCATION));
  }

  public void testOdsConverter(File workbookFile) throws IOException, InvalidFormatException {

    File testCsvFile = FileUtils.getClasspathFile(CSV_TEST_FILE_LOCATION);
    File testFile = folder.newFile();
    int numberOfLines = SpreadsheetConverters.convertOdsToCSV(workbookFile.toPath(), testFile.toPath());

    assertTrue(org.apache.commons.io.FileUtils.contentEqualsIgnoreEOL(testFile, testCsvFile, "UTF-8"));
    assertEquals(6, numberOfLines);
  }

}
