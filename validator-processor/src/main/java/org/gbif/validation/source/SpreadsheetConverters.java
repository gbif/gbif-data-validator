package org.gbif.validation.source;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

/**
 * Converters used to convert a spreadsheet into a tabular format (CSV).
 *
 */
public class SpreadsheetConverters {

  /**
   * Convert an Excel file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @throws IOException
   */
  public static void convertExcelToCSV(Path workbookFile, Path csvFile) throws IOException {
    ExcelConverter excelConverter = new ExcelConverter();
    try {
      excelConverter.convertToCSV(workbookFile, csvFile);
    } catch (InvalidFormatException e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert an ODF(Open Document Format) file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @throws IOException
   */
  public static void convertOdsToCSV(Path workbookFile, Path csvFile) throws IOException {
    OdsConverter odsConverter = new OdsConverter();
    odsConverter.convertToCSV(workbookFile, csvFile);
  }

}
