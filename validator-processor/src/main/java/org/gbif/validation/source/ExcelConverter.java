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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  private static final int FIRST_SHEET_INDEX = 0;

  private final DataFormatter formatter;

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
   * @param sheetSelector
   * @throws IOException
   * @throws InvalidFormatException Thrown if invalid xml is found whilst parsing an input SpreadsheetML file.
   */
  public void convertToCSV(Path workbookFile, Path csvFile, Function<List<String>, Optional<String>> sheetSelector)
          throws IOException, InvalidFormatException {

    try (FileInputStream fis = new FileInputStream(workbookFile.toFile());
         ICsvListWriter csvWriter = new CsvListWriter(new FileWriter(csvFile.toFile()),
                                                      CsvPreference.STANDARD_PREFERENCE)) {

      Workbook workbook = WorkbookFactory.create(fis);
      convertToCSV(workbook, csvWriter, sheetSelector);

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
   * @param sheetSelector
   * @throws IOException
   */
  private void convertToCSV(Workbook workbook, ICsvListWriter csvWriter, Function<List<String>, Optional<String>> sheetSelector) throws IOException {
    Objects.requireNonNull(workbook, "workbook shall be provided");
    Objects.requireNonNull(csvWriter, "csvWriter shall be provided");

    if (workbook.getNumberOfSheets() == 0) {
      LOG.warn("No sheet found in the workbook");
      return;
    }

    int sheetIndex = FIRST_SHEET_INDEX;
    if (workbook.getNumberOfSheets() > 1) {
      List<String> sheetNames = IntStream.range(0, workbook.getNumberOfSheets())
              .mapToObj( idx -> workbook.getSheetName(idx))
              .collect(Collectors.toList());
      //run the sheetSelector function to see if we can get a sheet name from it otherwise, keep the first sheet
      Optional<String> sheetName = sheetSelector.apply(sheetNames);
      if(sheetName.isPresent()) {
        sheetIndex = sheetName.map(name -> workbook.getSheetIndex(name)).get();
      }
      else{
        LOG.warn("Detected more than 1 sheet and can not get a selection from sheetSelector(), only reading the first one.");
      }
    }

    Sheet sheet = workbook.getSheetAt(sheetIndex); //get the first Sheet, and only get this
    if (sheet.getPhysicalNumberOfRows() > 0) {
      //pass the evaluator to other methods since it's unknown how expensive is to get one!
      writeSheetContent(csvWriter, sheet, workbook.getCreationHelper().createFormulaEvaluator());
    }
  }

  /**
   * Writes the content of a Sheet into the csvWriter.
   */
  private void writeSheetContent(ICsvListWriter csvWriter, Sheet sheet, FormulaEvaluator evaluator) throws IOException {
    //extract headers (first line)
    String[] headers = getHeaders(sheet, evaluator);
    csvWriter.writeHeader(headers);

    int rowSize = headers.length - 1; //this is done avoid the calculation on each call
    for(int j = 1; j <= sheet.getLastRowNum(); j++) {
      csvWriter.write(rowToCSV(sheet.getRow(j), evaluator, rowSize));
      if (j % FLUSH_INTERVAL == 0) {
        csvWriter.flush();
      }
    }
  }

  /**
   * Extract the first row from as the header.
   */
  private String[] getHeaders(Sheet sheet, FormulaEvaluator evaluator) {
    Row headerRow = sheet.getRow(0);
    //we want to loop until maxColumnIdx (if provided) even if it's greater than getLastCellNum()
    //we shall have the same number of entries on every line in the CSV
    List<String> headers = rowToCSV(headerRow, evaluator, headerRow.getLastCellNum());

    //reverse iteration (from the right side of the Workbook) to remove empty columns
    ListIterator<String> iterator = headers.listIterator(headers.size());
    while (iterator.hasPrevious()) {
      if (StringUtils.isBlank(iterator.previous())) {
        iterator.remove();
      }
    }
    return headers.toArray(new String[headers.size()]);
  }

  /**
   * Called to convert a row of cells into a line of data that can later be
   * output to the CSV file.
   *
   * @param row can be HSSFRow or XSSFRow classes or null
   * @param evaluator workbook formula evaluator
   * @param maxCellNum maximum number of cell to evaluate
   * @return
   */
  private List<String> rowToCSV(Row row, FormulaEvaluator evaluator, int maxCellNum) {
    List<String> csvLine = new ArrayList<>();
    if (row != null) {
      for (int i = 0; i <= maxCellNum; i++) {
        Cell cell = row.getCell(i);
        //add an empty string when we have no data
        if (cell == null) {
          csvLine.add("");
        } else {
          //getCellTypeEnum deprecation explanation: see https://bz.apache.org/bugzilla/show_bug.cgi?id=60228
          if (CellType.FORMULA == cell.getCellTypeEnum()) {
            csvLine.add(formatter.formatCellValue(cell, evaluator));
          } else if (cell.getCellTypeEnum() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            csvLine.add(DateUtil.getJavaDate(cell.getNumericCellValue(), false, UTC_TIMEZONE).toInstant().toString());
          } else {
            csvLine.add(formatter.formatCellValue(cell));
          }
        }
      }
    }
    return csvLine;
  }

}
