package org.gbif.validation.source;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.odftoolkit.simple.SpreadsheetDocument;
import org.odftoolkit.simple.table.Cell;
import org.odftoolkit.simple.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Experimental converter that reads an ODF(Open Document Format) spreadsheet (.ods) file and produces a csv file.
 *
 * Issues: odftoolkit table.getColumnCount() and table.getRowCount() are not working.
 * See https://issues.apache.org/jira/browse/ODFTOOLKIT-381
 *
 * Limitation: the conversion will stop at the first empty line.
 *
 */
class OdsConverter {

  private static final Logger LOG = LoggerFactory.getLogger(OdsConverter.class);
  private static final String DATE_VALUE_TYPE = "date";

  public void convertToCSV(Path workbookFile, Path csvFile) throws IOException {

    try (FileInputStream fis = new FileInputStream(workbookFile.toFile());
         ICsvListWriter csvWriter = new CsvListWriter(new FileWriter(csvFile.toFile()),
                 CsvPreference.STANDARD_PREFERENCE)) {

      SpreadsheetDocument doc = SpreadsheetDocument.loadDocument(fis);
      convertToCSV(doc, csvWriter);

      //ensure to flush remaining content to csvWriter
      csvWriter.flush();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   *
   * Convert a {@link SpreadsheetDocument} to a CSV file.
   * Writes CSV using using {@link ICsvListWriter}.
   *
   * @param spreadsheetDocument
   * @param csvWriter
   * @throws IOException
   */
  private static void convertToCSV(SpreadsheetDocument spreadsheetDocument, ICsvListWriter csvWriter) throws IOException {
    Objects.requireNonNull(spreadsheetDocument, "spreadsheetDocument shall be provided");
    Objects.requireNonNull(csvWriter, "csvWriter shall be provided");

    if (spreadsheetDocument.getSheetCount() == 0) {
      LOG.warn("No sheet found in the spreadsheetDocument");
      return;
    } //we work only on one sheet
    if(spreadsheetDocument.getSheetCount() > 1) {
      LOG.warn("Detected more than 1 sheet, only reading the first one.");
    }

    Table table = spreadsheetDocument.getSheetByIndex(0);

    List<String> headers = extractWhile(table, 0, cell -> cell != null && StringUtils.isNotBlank(cell.getStringValue()));
    csvWriter.writeHeader(headers.toArray(new String[headers.size()]));

    boolean hasContent = true;
    int rowIdx = 1;
    while (hasContent) {
      List<String> line = extractLine(table, rowIdx, headers.size());
      hasContent = !line.stream().allMatch(StringUtils::isBlank);
      if (hasContent) {
        csvWriter.write(line);
      }
      rowIdx++;
    }
  }

  /**
   * Extract line of data from a {@link Table}.
   *
   * @param table
   * @param row
   * @param expectedNumberOfColumns
   * @return
   */
  private static List<String> extractLine(Table table, int row, int expectedNumberOfColumns) {
    List<String> lineData = new ArrayList<>(expectedNumberOfColumns);

    for (int colIdx = 0; colIdx < expectedNumberOfColumns; colIdx++) {
      Cell cell = table.getCellByPosition(colIdx, row);

      if(cell == null){
        lineData.add("");
      } else if (DATE_VALUE_TYPE.equalsIgnoreCase(cell.getValueType())) {
        //we should document what "H:" means
        Instant instant = cell.getFormatString().indexOf("H:") > 0 ? cell.getDateTimeValue().toInstant():
                                                                     cell.getDateValue().toInstant();

        //we need to use systemDefault ZoneId since the date we get from the library used it (by using Calendar).
        LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        //set it to UTC for format it like 1990-01-02T00:00:00Z
        lineData.add(date.atZone(ZoneId.of("UTC")).toInstant().toString());
      } else {
        lineData.add(cell.getStringValue());
      }
    }

    return lineData;
  }

  /**
   * Extract data on a line while the provided function returns true.
   * @param table
   * @param row
   * @param check function to run on a Cell object to know if we should continue to extract data on the row.
   * @return
   */
  private static List<String> extractWhile(Table table, int row, Function<Cell, Boolean> check) {
    List<String> headers = new ArrayList<>();

    int colIdx = 0;
    while(check.apply(table.getCellByPosition(colIdx, row))) {
      headers.add(table.getCellByPosition(colIdx, row).getStringValue());
      colIdx++;
    }
    return headers;
  }

}
