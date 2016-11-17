package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.FileFormat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represents a single source data in a file. The source of data can be a portion of a bigger source.
 * It can also represent a tabular view of other formats (spreadsheet).
 *
 */
public class DataFile {

  private Path filePath;
  private FileFormat fileFormat;
  private String contentType;

  private String sourceFileName;

  private Term[] columns;
  private Term rowType;

  private Optional<Integer> fileLineOffset = Optional.empty();

  private Character delimiterChar;
  private Integer numOfLines;

  private boolean hasHeaders;

  private final Optional<DataFile> isAlternateViewOf;

  public DataFile() {
    this(null);
  }

  /**
   * Constructor used to get a new {@link DataFile} that represents an alternative view of another {@link DataFile}.
   *
   * @param isAlternateViewOf the parent {@link DataFile}
   */
  public DataFile(DataFile isAlternateViewOf) {
    this.isAlternateViewOf = Optional.ofNullable(isAlternateViewOf);
  }

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public void setDelimiterChar(Character delimiterChar) {
    this.delimiterChar = delimiterChar;
  }

  public Term[] getColumns() {
    return columns;
  }

  public void setColumns(Term[] columns) {
    this.columns = columns;
  }

  public Term getRowType() {
    return rowType;
  }

  public void setRowType(Term rowType) {
    this.rowType = rowType;
  }

  public void setFilePath(Path filePath) {
    this.filePath = filePath;
  }

  public void setNumOfLines(Integer numOfLines) {
    this.numOfLines = numOfLines;
  }

  /**
   * Name of the file as received. For safety reason this name should only be used to display.
   * @param sourceFileName
   */
  public void setSourceFileName(String sourceFileName) {
    this.sourceFileName = sourceFileName;
  }

  public Integer getNumOfLines() {
    return numOfLines;
  }

  /**
   * File name as provided. For safety reason this name should only be used to display.
   *
   */
  @Nullable
  public String getSourceFileName() {
    return sourceFileName;
  }


  /**
   * Path to the working file stored with a generated file name.
   *
   * @return safe, path to generated filename
   */
  public Path getFilePath() {
    return filePath;
  }

  /**
   * If this {@link DataFile} represents a part of a bigger file, the offset of lines relative to the
   * source file.
   * @return
   */
  public Optional<Integer> getFileLineOffset() {
    return fileLineOffset;
  }

  public void setFileLineOffset(Optional<Integer> fileLineOffset) {
    this.fileLineOffset = fileLineOffset;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  public boolean isHasHeaders() {
    return hasHeaders;
  }

  public void setHasHeaders(boolean hasHeaders) {
    this.hasHeaders = hasHeaders;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public Optional<DataFile> isAlternateViewOf() {
    return isAlternateViewOf;
  }

  @Override
  public String toString() {
    return "DataFile{" +
            "filePath=" + filePath +
            ", fileFormat=" + fileFormat +
            ", sourceFileName=" + sourceFileName +
            ", columns=" + Arrays.toString(columns) +
            ", rowType=" + rowType +
            ", delimiterChar='" + delimiterChar + '\'' +
            ", numOfLines=" + numOfLines +
            ", fileLineOffset=" + fileLineOffset +
            ", hasHeaders=" + hasHeaders +
            ", isAlternateViewOf=" + isAlternateViewOf +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataFile dataFile = (DataFile) o;
    return hasHeaders == dataFile.hasHeaders &&
            Objects.equals(delimiterChar, dataFile.delimiterChar) &&
            Arrays.equals(columns, dataFile.columns) &&
            Objects.equals(rowType, dataFile.rowType) &&
            Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(fileFormat, dataFile.fileFormat) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(numOfLines, dataFile.numOfLines) &&
            Objects.equals(fileLineOffset, dataFile.fileLineOffset) &&
            Objects.equals(isAlternateViewOf, dataFile.isAlternateViewOf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            fileFormat,
            sourceFileName,
            columns,
            rowType,
            delimiterChar,
            numOfLines,
            fileLineOffset,
            hasHeaders,
            isAlternateViewOf);
  }

}
