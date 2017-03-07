package org.gbif.validation.source;

import java.nio.file.Path;

/**
 * Contains the result of a spreadsheet conversion.
 */
public class SpreadsheetConversionResult {

  private final Path sourcePath;
  private final Path resultPath;
  private final Character delimiterChar;
  private final Character quoteChar;
  private final Integer numOfLines;

  /**
   *
   * @param sourcePath
   * @param resultPath
   * @param delimiterChar delimiter char used in the result file
   * @param quoteChar quote char used in the result file
   * @param numOfLines number of line written on the result file
   */
  public SpreadsheetConversionResult(Path sourcePath, Path resultPath, Character delimiterChar,
                              Character quoteChar, Integer numOfLines) {
    this.sourcePath = sourcePath;
    this.resultPath = resultPath;
    this.delimiterChar = delimiterChar;
    this.quoteChar = quoteChar;
    this.numOfLines = numOfLines;
  }

  public Path getSourcePath() {
    return sourcePath;
  }

  public Path getResultPath() {
    return resultPath;
  }

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public Character getQuoteChar() {
    return quoteChar;
  }

  public Integer getNumOfLines() {
    return numOfLines;
  }
}
