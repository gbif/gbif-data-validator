package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.vocabulary.DwcFileType;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import javax.annotation.Nullable;


/**
 * Represents the workable unit for the validation. It can point to a file or a portion of a bigger file.
 * It can also represent a tabular view of other formats (spreadsheet).
 *
 * Expected  LF (\n) end of line character.
 *
 * This class is thread-safe and immutable.
 */
public class TabularDataFile {

  private final RowTypeKey rowTypeKey;

  private final Path filePath;
  private final String sourceFileName;

  private final Term[] columns;

  private final TermIndex recordIdentifier;
  private final Map<Term, String> defaultValues;

  private final Integer fileLineOffset;
  private final boolean hasHeaders;

  private final Charset characterEncoding;
  private final Character delimiterChar;
  private final Character quoteChar;
  private final Integer numOfLines;
  private final Integer numOfLinesWithData;

  /**
   * Complete constructor of {@link TabularDataFile}
   *
   * @param filePath path where the file is located
   * @param sourceFileName Name of the file as received. For safety reason this name should only be used to display.
   * @param rowTypeKey the rowTypeKey (rowType + dwcFileType) of this file in the context of DarwinCore
   * @param columns columns of the file, in the right order
   * @param recordIdentifier {@link Term} and its index used to uniquely identifier a record within the file
   * @param defaultValues default values to use for specific {@Term}
   * @param fileLineOffset if the file represents a part of a bigger file, the offset (in line) relative to the parent file
   * @param hasHeaders does the first line of this file represents the headers or no
   * @param characterEncoding
   * @param delimiterChar character used to delimit each value (cell) in the file
   * @param quoteChar
   * @param numOfLines
   * @param numOfLinesWithData
   */
  public TabularDataFile(Path filePath, String sourceFileName,
                         RowTypeKey rowTypeKey, Term[] columns,
                         @Nullable TermIndex recordIdentifier,
                         @Nullable Map<Term, String> defaultValues,
                         @Nullable Integer fileLineOffset, boolean hasHeaders,
                         Charset characterEncoding,
                         Character delimiterChar, Character quoteChar, Integer numOfLines, Integer numOfLinesWithData) {
    Objects.requireNonNull(rowTypeKey, "rowTypeKey shall be provided");

    this.filePath = filePath;
    this.sourceFileName = sourceFileName;
    this.rowTypeKey = rowTypeKey;
    this.columns = Arrays.copyOf(columns, columns.length);
    this.recordIdentifier = recordIdentifier;
    this.defaultValues = (defaultValues != null) ? new HashMap<>(defaultValues) : null;
    this.fileLineOffset = fileLineOffset;
    this.hasHeaders = hasHeaders;
    this.characterEncoding = characterEncoding;
    this.delimiterChar = delimiterChar;
    this.quoteChar = quoteChar;
    this.numOfLines = numOfLines;
    this.numOfLinesWithData = numOfLinesWithData;
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
   * File name as provided. For safety reason this name should only be used to display.
   *
   */
  @Nullable
  public String getSourceFileName() {
    return sourceFileName;
  }

  public Charset getCharacterEncoding() {
    return characterEncoding;
  }

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public Character getQuoteChar() {
    return quoteChar;
  }

  public DwcFileType getDwcFileType() {
    return rowTypeKey.getDwcFileType();
  }

  public RowTypeKey getRowTypeKey() {
    return rowTypeKey;
  }

  public Term[] getColumns() {
    return columns;
  }

  public Optional<TermIndex> getRecordIdentifier() {
    return Optional.ofNullable(recordIdentifier);
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
    return Optional.ofNullable(fileLineOffset);
  }

  /**
   * Get the default value to use for some terms (if defined).
   * @return
   */
  public Optional<Map<Term, String>> getDefaultValues() {
    return Optional.ofNullable(defaultValues);
  }

  public Integer getNumOfLinesWithData() {
    return numOfLinesWithData;
  }

  /**
   * Get the index of a {@link Term} or OptionalInt.empty if the Term can not be found.
   *
   * @param term
   *
   * @return
   */
  public OptionalInt getIndexOf(Term term) {
    if (columns == null) {
      return OptionalInt.empty();
    }
    return IntStream.range(0, columns.length)
            .filter(idx -> term.equals(columns[idx]))
            .findAny();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TabularDataFile dataFile = (TabularDataFile) o;
    return hasHeaders == dataFile.hasHeaders &&
            Objects.equals(characterEncoding, dataFile.characterEncoding) &&
            Objects.equals(delimiterChar, dataFile.delimiterChar) &&
            Objects.equals(quoteChar, dataFile.quoteChar) &&
            Arrays.equals(columns, dataFile.columns) &&
            Objects.equals(recordIdentifier, dataFile.recordIdentifier) &&
            Objects.equals(rowTypeKey, dataFile.rowTypeKey) &&
            Objects.equals(defaultValues, dataFile.defaultValues) &&
            Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(numOfLines, dataFile.numOfLines) &&
            Objects.equals(numOfLinesWithData, dataFile.numOfLinesWithData) &&
            Objects.equals(fileLineOffset, dataFile.fileLineOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            sourceFileName,
            columns,
            recordIdentifier,
            rowTypeKey,
            defaultValues,
            characterEncoding,
            delimiterChar,
            quoteChar,
            numOfLines,
            numOfLinesWithData,
            fileLineOffset,
            hasHeaders);
  }

}
