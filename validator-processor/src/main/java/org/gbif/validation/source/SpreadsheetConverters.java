package org.gbif.validation.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

/**
 * Converters used to convert a spreadsheet into a tabular format (CSV).
 *
 */
public class SpreadsheetConverters {

  public static final Character QUOTE_CHAR = '\"';
  public static final Character DELIMITER_CHAR = ',';

  /**
   * Utility class can't be instantiated.
   */
  private SpreadsheetConverters() {
    //empty constructor
  }

  /**
   * Convert an Excel file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @param sheetSelector function used to select the right sheet to convert (if more than one sheet is found)
   * @return number of lines converted
   * @throws IOException
   */
  public static int convertExcelToCSV(Path workbookFile, Path csvFile,
                                       Function<List<String>, Optional<String>> sheetSelector) throws IOException {
    try {
      ExcelConverter excelConverter = new ExcelConverter();
      return excelConverter.convertToCSV(workbookFile, csvFile, sheetSelector);
    } catch (InvalidFormatException e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert an ODF(Open Document Format) file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @return number of lines converted
   * @throws IOException
   */
  public static int convertOdsToCSV(Path workbookFile, Path csvFile) throws IOException {
    return OdsConverter.convertToCSV(workbookFile, csvFile);
  }

}
