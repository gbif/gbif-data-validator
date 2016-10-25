package org.gbif.validation.api;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

public class DataFile {

  private Character delimiterChar = '\t';

  private String[] columns;

  private String fileName;

  private Integer numOfLines;

  private Integer fileLineOffset;

  private boolean hasHeaders;

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public void setDelimiterChar(Character delimiterChar) {
    this.delimiterChar = delimiterChar;
  }

  public String[] getColumns() {
    return columns;
  }

  public void setColumns(String[] columns) {
    this.columns = columns;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Integer getNumOfLines() {
    return numOfLines;
  }

  @Nullable
  public void setNumOfLines(Integer numOfLines) {
    this.numOfLines = numOfLines;
  }

  /**
   * If this {@link DataFile} represents a part of a bigger file, the offset of lines relative to the
   * source file.
   * @return
   */
  public Integer getFileLineOffset() {
    return fileLineOffset;
  }

  public void setFileLineOffset(Integer fileLineOffset) {
    this.fileLineOffset = fileLineOffset;
  }

  public boolean isHasHeaders() {
    return hasHeaders;
  }

  public void setHasHeaders(boolean hasHeaders) {
    this.hasHeaders = hasHeaders;
  }

  public void loadHeaders() {
    columns = readHeader();
  }

  @Override
  public String toString() {
    return "DataFile{" +
           "delimiterChar=" + delimiterChar +
           ", columns=" + Arrays.toString(columns) +
           ", fileName='" + fileName + '\'' +
           ", numOfLines=" + numOfLines +
           ", lineOffset=" + fileLineOffset +
           ", hasHeaders=" + hasHeaders +
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
           Objects.equals(fileName, dataFile.fileName) &&
           Objects.equals(numOfLines, dataFile.numOfLines) &&
           Objects.equals(fileLineOffset, dataFile.fileLineOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delimiterChar, columns, fileName, numOfLines, fileLineOffset, hasHeaders);
  }

  /**
   * Reads the first line of a file and return it as the header.
   * @return
   */
  private String[] readHeader() {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      return br.readLine().split(delimiterChar.toString());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  }
}