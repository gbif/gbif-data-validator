package org.gbif.validation.api;

import org.gbif.validation.api.model.FileFormat;

import java.nio.file.Path;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * Represents data held in a file or a folder containing multiple files.
 * This class is thread-safe and immutable.
 */
public class DataFile {

  private final Path filePath;
  private final String sourceFileName;
  private final FileFormat fileFormat;
  private final String contentType;

  /**
   * Complete constructor of {@link DataFile}
   *
   * @param filePath       path where the file is located
   * @param sourceFileName Name of the file as received. For safety reason this name should only be used to display.
   * @param fileFormat
   * @param contentType    as received by the "resource" layer
   */
  public DataFile(Path filePath, String sourceFileName, FileFormat fileFormat, String contentType) {
    this.filePath = filePath;
    this.sourceFileName = sourceFileName;
    this.fileFormat = fileFormat;
    this.contentType = contentType;
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

  public FileFormat getFileFormat() {
    return fileFormat;
  }


  public String getContentType() {
    return contentType;
  }


  @Override
  public String toString() {
    return "DataFile{" +
            "filePath=" + filePath +
            ", sourceFileName=" + sourceFileName +
            ", fileFormat=" + fileFormat +
            ", contentType=" + contentType +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataFile dataFile = (DataFile) o;
    return Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(fileFormat, dataFile.fileFormat) &&
            Objects.equals(contentType, dataFile.contentType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            sourceFileName,
            fileFormat,
            contentType);
  }

}
