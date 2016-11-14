package org.gbif.validation.source;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 *
 * This converter reads a Workbook (.xlsx, .xls) file and produces a csv file.
 *
 * Inspired from Apache POI example:
 * http://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/ss/examples/ToCSV.java
 */
class ExcelConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ExcelConverter.class);
  private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
  private static final int FLUSH_INTERVAL = 1000;

  private DataFormatter formatter;

  /**
   * Get a new ExcelConverter instance.
   *
   * @return
   */
  ExcelConverter() {
    formatter = new DataFormatter(Locale.ENGLISH, true);
  }

  /**
   * Convert a workbook (binary .xls or SpreadsheetML .xlsx format) to csv.
   *
   * @param workbookFile Path to the  workbook file
   * @param csvFile Path to the csv file to produce
   * @throws IOException
   * @throws InvalidFormatException Thrown if invalid xml is found whilst parsing an input SpreadsheetML file.
   */
  public void convertToCSV(Path workbookFile, Path csvFile) throws IOException, InvalidFormatException {

    try (FileInputStream fis = new FileInputStream(workbookFile.toFile());
         ICsvListWriter csvWriter = new CsvListWriter(new FileWriter(csvFile.toFile()),
                 CsvPreference.STANDARD_PREFERENCE)) {

      Workbook workbook = WorkbookFactory.create(fis);
      convertToCSV(workbook, csvWriter);

      //ensure to flush remaining content to csvWriter
      csvWriter.flush();
    }
  }


  /**
   *
   * Convert a {@link Workbook} to a CSV file using {@link ICsvListWriter}
   *
   * @param workbook
   * @param csvWriter
   * @throws IOException
   */
  private void convertToCSV(Workbook workbook, ICsvListWriter csvWriter) throws IOException {
    Objects.requireNonNull(workbook, "workbook shall be provided");
    Objects.requireNonNull(csvWriter, "csvWriter shall be provided");

    if( workbook.getNumberOfSheets() == 0 ) {
      LOG.warn("No sheet found in the workbook");
      return;
    } //we work only on one sheet
    else  if( workbook.getNumberOfSheets() > 1) {
      LOG.warn("Detected more than 1 sheet, only reading the first one.");
    }

    Sheet sheet = workbook.getSheetAt(0);
    FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
    if(sheet.getPhysicalNumberOfRows() > 0) {
      Row row;
      //extract headers (first line)
      row = sheet.getRow(0);
      List<String> headers = rowToCSV(row, evaluator, Optional.empty());

      //reverse iteration (from the right side of the Workbook) to remove empty columns
      ListIterator<String> iterator = headers.listIterator(headers.size());
      while (iterator.hasPrevious()) {
        if(StringUtils.isBlank(iterator.previous())){
          iterator.remove();
        }
      }
      csvWriter.writeHeader(headers.toArray(new String[headers.size()]));

      int lastRowNum = sheet.getLastRowNum();
      int nextFlush = FLUSH_INTERVAL;
      for(int j = 1; j <= lastRowNum; j++) {
        row = sheet.getRow(j);
        csvWriter.write(rowToCSV(row, evaluator, Optional.of(headers.size() - 1)));
        if(j == nextFlush){
          csvWriter.flush();
          nextFlush += FLUSH_INTERVAL;
        }
      }
    }
  }

  /**
   * Called to convert a row of cells into a line of data that can later be
   * output to the CSV file.
   *
   * @param row can be HSSFRow or XSSFRow classes or null
   * @param evaluator
   * @param maxColumnIdx
   * @return
   */
  private List<String> rowToCSV(@Nullable Row row, FormulaEvaluator evaluator, Optional<Integer> maxColumnIdx) {
    List<String> csvLine = new ArrayList<>();

    if(row != null) {
      Cell cell;
      //we want to loop until maxColumnIdx (if provided) even if it's greater than getLastCellNum()
      //we shall have the same number of entries on every line in the CSV
      int maxCellNum = maxColumnIdx.orElse(Short.valueOf(row.getLastCellNum()).intValue());
      for(int i = 0; i <= maxCellNum; i++) {
        cell = row.getCell(i);
        //add an empty string when we have no data
        if(cell == null) {
          csvLine.add("");
        }
        else {
          //getCellTypeEnum deprecation explanation: see https://bz.apache.org/bugzilla/show_bug.cgi?id=60228
          if(CellType.FORMULA == cell.getCellTypeEnum()) {
            csvLine.add(this.formatter.formatCellValue(cell, evaluator));
          }
          else if(cell.getCellTypeEnum() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)){
            csvLine.add(DateUtil.getJavaDate(cell.getNumericCellValue(), false, UTC_TIMEZONE).toInstant().toString());
          }
          else{
            csvLine.add(this.formatter.formatCellValue(cell));
          }
        }
      }
    }
    return csvLine;
  }

}
