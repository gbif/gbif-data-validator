package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.FileFormat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represents data held in a file. It can be a portion of a bigger file.
 * It can also represent a tabular view of other formats (spreadsheet).
 *
 * A {@link DataFile} can point to a parent (in a conceptual way).
 * e.g. DarwinCore Archive folder is the parent of its core file.
 */
public class DataFile {

  private Path filePath;
  private FileFormat fileFormat;
  private String contentType;

  private String sourceFileName;

  private Term[] columns;
  private Term rowType;

  private boolean isCore = true;

  private Optional<Integer> fileLineOffset = Optional.empty();
  private boolean hasHeaders = false;

  private Character delimiterChar;
  private Integer numOfLines;

  private final Optional<DataFile> parent;

  public DataFile() {
    this(null);
  }

  /**
   * Constructor used to get a new {@link DataFile} that represents an alternative view of another {@link DataFile}.
   *
   * @param parent the parent {@link DataFile}
   */
  public DataFile(DataFile parent) {
    this.parent = Optional.ofNullable(parent);
  }

  public static DataFile copyFromParent(DataFile parentDataFile) {
    DataFile newDataFile = new DataFile(parentDataFile);
    return copyInto(parentDataFile, newDataFile);
  }
  /**
   * Get a copy of a {@link DataFile}.
   * @param dataFile
   * @return
   */
  public static DataFile copy(DataFile dataFile) {
    DataFile newDataFile = new DataFile(dataFile.parent.orElse(null));
    return copyInto(dataFile, newDataFile);
  }

  /**
   * Mutable method to copy all properties of one {@link DataFile} into another.
   *
   * @param srcDataFile
   * @param destDataFile
   */
  private static DataFile copyInto(DataFile srcDataFile, DataFile destDataFile) {
    destDataFile.filePath = srcDataFile.filePath;
    destDataFile.fileFormat = srcDataFile.fileFormat;
    destDataFile.contentType = srcDataFile.contentType;

    destDataFile.sourceFileName = srcDataFile.sourceFileName;

    destDataFile.columns = srcDataFile.columns;
    destDataFile.rowType = srcDataFile.rowType;
    destDataFile.isCore = srcDataFile.isCore;

    destDataFile.fileLineOffset = srcDataFile.fileLineOffset;
    destDataFile.hasHeaders = srcDataFile.hasHeaders;

    destDataFile.delimiterChar = srcDataFile.delimiterChar;
    destDataFile.numOfLines = srcDataFile.numOfLines;

    return destDataFile;
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

  /**
   * Does this {@link DataFile} represent the "core" of a DarwinCore start schema representation.
   * If the {@link DataFile} is not inside DarwinCore Archive it is considered the "core" file.
   * Default value: true
   * @return
   */
  public boolean isCore() {
    return isCore;
  }

  public void setCore(boolean core) {
    isCore = core;
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

  /**
   * Does this {@link DataFile} contain headers on the first row.
   * Default value: false
   * @return
   */
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

  public Optional<DataFile> getParent() {
    return parent;
  }

  @Override
  public String toString() {
    return "DataFile{" +
            "filePath=" + filePath +
            ", fileFormat=" + fileFormat +
            ", sourceFileName=" + sourceFileName +
            ", columns=" + Arrays.toString(columns) +
            ", rowType=" + rowType +
            ", isCore=" + isCore +
            ", delimiterChar='" + delimiterChar + '\'' +
            ", numOfLines=" + numOfLines +
            ", fileLineOffset=" + fileLineOffset +
            ", hasHeaders=" + hasHeaders +
            ", parent=" + parent +
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
            Objects.equals(isCore, dataFile.isCore) &&
            Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(fileFormat, dataFile.fileFormat) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(numOfLines, dataFile.numOfLines) &&
            Objects.equals(fileLineOffset, dataFile.fileLineOffset) &&
            Objects.equals(parent, dataFile.parent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            fileFormat,
            sourceFileName,
            columns,
            rowType,
            isCore,
            delimiterChar,
            numOfLines,
            fileLineOffset,
            hasHeaders,
            parent);
  }

}
