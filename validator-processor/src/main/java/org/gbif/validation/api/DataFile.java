package org.gbif.validation.api;

import org.gbif.validation.api.model.FileFormat;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;


/**
 * Represents data held in a file or a folder containing multiple files.
 * This class is thread-safe and immutable.
 */
public class DataFile {

  protected final Path filePath;
  protected final String sourceFileName;
  protected final FileFormat fileFormat;
  protected final String contentType;
  protected final Optional<Path> metadataFolder;

  /**
   * See {@link #DataFile(Path, String, FileFormat, String, Optional)}
   * @param filePath
   * @param sourceFileName
   * @param fileFormat
   * @param contentType
   */
  public DataFile(Path filePath, String sourceFileName, FileFormat fileFormat, String contentType) {
    this(filePath, sourceFileName, fileFormat, contentType, Optional.empty());
  }

  /**
   * Complete constructor of {@link DataFile}
   *
   * @param filePath       path where the file is located
   * @param sourceFileName Name of the file as received. For safety reason this name should only be used to display.
   * @param fileFormat
   * @param contentType    as received by the "resource" layer
   * @param metadataFolder optionally, a DataFile can contain (or point to) a metadata folder
   */
  public DataFile(Path filePath, String sourceFileName, FileFormat fileFormat, String contentType,
                  Optional<Path> metadataFolder) {
    this.filePath = filePath;
    this.sourceFileName = sourceFileName;
    this.fileFormat = fileFormat;
    this.contentType = contentType;
    this.metadataFolder = metadataFolder;
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

  /**
   * A folder containing metadata about this {@link DataFile}.
   * @return
   */
  public Optional<Path> getMetadataFolder() {
    return metadataFolder;
  }

  @Override
  public String toString() {
    return "DataFile{" +
            "filePath=" + filePath +
            ", sourceFileName=" + sourceFileName +
            ", fileFormat=" + fileFormat +
            ", contentType=" + contentType +
            ", metadataFolder=" + metadataFolder +
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
            Objects.equals(contentType, dataFile.contentType) &&
            Objects.equals(metadataFolder, dataFile.metadataFolder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            filePath,
            sourceFileName,
            fileFormat,
            contentType,
            metadataFolder);
  }

}
