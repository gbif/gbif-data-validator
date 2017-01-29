package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.FileFormat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


/**
 * Represents the workable unit for the validation. It can point to a file or a portion of a bigger file.
 * It can also represent a tabular view of other formats (spreadsheet).
 *
 * A {@link TabularDataFile} can point to a parent (in a conceptual way).
 * e.g. DarwinCore Archive folder is the parent of its core file.
 *
 * This class is thread-safe and immutable.
 */
public class TabularDataFile extends DataFile {

  private final Term rowType;
  private final DwcFileType type;
  private final Term[] columns;
  private final Optional<Term> recordIdentifier;

  private final Optional<Map<Term, String>> defaultValues;

  private final Optional<Integer> fileLineOffset;
  private final boolean hasHeaders;

  private final Character delimiterChar;
  private final Integer numOfLines;

  private final Optional<DataFile> parent;

  /**
   * Complete constructor of {@link TabularDataFile}
   *
   * @param filePath path where the file is located
   * @param sourceFileName Name of the file as received. For safety reason this name should only be used to display.
   * @param fileFormat
   * @param contentType
   * @param rowType the rowType (sometimes called "class") of this file in the context of DarwinCore
   * @param type the type of file in the context of DarwinCore
   * @param columns columns of the file, in the right order
   * @param recordIdentifier {@Term} used to uniquely identifier a record within the file
   * @param defaultValues default values to use for specific {@Term}
   * @param fileLineOffset if the file represents a part of a bigger file, the offset (in line) relative to the parent file
   * @param hasHeaders does the first line of this file represents the headers or no
   * @param delimiterChar character used to delimit each value (cell) in the file
   * @param numOfLines
   * @param metadataFolder
   * @param parent
   */
  public TabularDataFile(Path filePath, String sourceFileName, FileFormat fileFormat, String contentType,
                         Term rowType, DwcFileType type, Term[] columns, Optional<Term> recordIdentifier,
                         Optional<Map<Term, String>> defaultValues,
                         Optional<Integer> fileLineOffset, boolean hasHeaders, Character delimiterChar, Integer numOfLines,
                         Optional<Path> metadataFolder, Optional<DataFile> parent) {
    super(filePath, sourceFileName, fileFormat, contentType, metadataFolder);

    this.rowType = rowType;
    this.type = type;
    this.columns = Arrays.copyOf(columns, columns.length);
    this.recordIdentifier = recordIdentifier;
    this.defaultValues = defaultValues.map( dv -> new HashMap<>(dv));
    this.fileLineOffset = fileLineOffset;
    this.hasHeaders = hasHeaders;
    this.delimiterChar = delimiterChar;
    this.numOfLines = numOfLines;
    this.parent = parent;
  }

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public Term getRowType() {
    return rowType;
  }

  public DwcFileType getType() {
    return type;
  }

  public Term[] getColumns() {
    return columns;
  }

  public Optional<Term> getRecordIdentifier() {
    return recordIdentifier;
  }

  public Integer getNumOfLines() {
    return numOfLines;
  }

  /**
   * Does this {@link DataFile} contain headers on the first row.
   * Default value: false
   * @return
   */
  public boolean isHasHeaders() {
    return hasHeaders;
  }

  /**
   * If this {@link DataFile} represents a part of a bigger file, the offset of lines relative to the
   * source file.
   * @return
   */
  public Optional<Integer> getFileLineOffset() {
    return fileLineOffset;
  }

  /**
   * Get the default value to use for some terms (if defined).
   * @return
   */
  public Optional<Map<Term, String>> getDefaultValues() {
    return defaultValues;
  }

  public Optional<DataFile> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TabularDataFile dataFile = (TabularDataFile) o;
    return hasHeaders == dataFile.hasHeaders &&
            Objects.equals(delimiterChar, dataFile.delimiterChar) &&
            Arrays.equals(columns, dataFile.columns) &&
            Objects.equals(recordIdentifier, dataFile.recordIdentifier) &&
            Objects.equals(rowType, dataFile.rowType) &&
            Objects.equals(defaultValues, dataFile.defaultValues) &&
            Objects.equals(type, dataFile.type) &&
            Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(fileFormat, dataFile.fileFormat) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(numOfLines, dataFile.numOfLines) &&
            Objects.equals(fileLineOffset, dataFile.fileLineOffset) &&
            Objects.equals(metadataFolder, dataFile.metadataFolder) &&
            Objects.equals(parent, dataFile.parent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            fileFormat,
            sourceFileName,
            columns,
            recordIdentifier,
            rowType,
            defaultValues,
            type,
            delimiterChar,
            numOfLines,
            fileLineOffset,
            hasHeaders,
            metadataFolder,
            parent);
  }

}
