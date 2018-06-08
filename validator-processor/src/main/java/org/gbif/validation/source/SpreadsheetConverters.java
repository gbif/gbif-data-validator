package org.gbif.validation.source;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.gbif.utils.file.spreadsheet.CsvSpreadsheetConsumer;
import org.gbif.utils.file.spreadsheet.ExcelConverter;
import org.gbif.utils.file.spreadsheet.ExcelXmlConverter;
import org.xml.sax.SAXException;

/**
 * Converters used to convert a spreadsheet into a tabular format (CSV).
 *
 */
public class SpreadsheetConverters {

  /**
   * Utility class can't be instantiated.
   */
  private SpreadsheetConverters() {
    //empty constructor
  }

  /**
   * Convert an Excel XML file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @return number of lines converted
   * @throws IOException
   */
  public static SpreadsheetConversionResult convertExcelXmlToCSV(Path workbookFile, Path csvFile) throws IOException {
    try {
      long lines = ExcelXmlConverter.convert(workbookFile, new CsvSpreadsheetConsumer(new FileWriter(csvFile.toFile())));
      return new SpreadsheetConversionResult(workbookFile, csvFile, ',','"', (int) lines);
    } catch (SAXException | OpenXML4JException e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert an Excel file into a CSV file.
   *
   * @param workbookFile
   * @param csvFile
   * @return number of lines converted
   * @throws IOException
   */
  public static SpreadsheetConversionResult convertExcelToCSV(Path workbookFile, Path csvFile) throws IOException {
    try {
      long lines = ExcelConverter.convert(workbookFile, new CsvSpreadsheetConsumer(new FileWriter(csvFile.toFile())));
      return new SpreadsheetConversionResult(workbookFile, csvFile, ',','"', (int) lines);
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
  public static SpreadsheetConversionResult convertOdsToCSV(Path workbookFile, Path csvFile) throws IOException {
    return OdsConverter.convertToCSV(workbookFile, csvFile);
  }

}
