package org.gbif.validation.api;

import org.gbif.validation.api.vocabulary.FileFormat;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Represents data held in a file or a folder containing multiple files.
 * This class is thread-safe and immutable.
 */
public class DataFile {

  private final UUID key;
  private final Path filePath;
  private final String sourceFileName;
  private final FileFormat fileFormat;
  private final String receivedAsMediaType;
  private final String mediaType;

  /**
   * Complete constructor of {@link DataFile}
   *
   * @param key                 unique key for this datafile
   * @param filePath            path where the file is located
   * @param sourceFileName      Name of the file as received. For safety reason this name should only be used to
   *                            display.
   * @param fileFormat
   * @param receivedAsMediaType as received by the "resource" layer
   * @param mediaType           as detected by the file transfer manager
   */
  public DataFile(UUID key, Path filePath, String sourceFileName, FileFormat fileFormat, String receivedAsMediaType,
                  String mediaType) {
    Objects.requireNonNull(key, "key shall be provided");

    this.key = key;
    this.filePath = filePath;
    this.sourceFileName = sourceFileName;
    this.fileFormat = fileFormat;
    this.receivedAsMediaType = receivedAsMediaType;
    this.mediaType = mediaType;
  }

  public UUID getKey() {
    return key;
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

  public String getReceivedAsMediaType() {
    return receivedAsMediaType;
  }

  public String getMediaType() {
    return mediaType;
  }

  @Override
  public String toString() {
    return "DataFile{" +
            "key=" + key +
            "filePath=" + filePath +
            ", sourceFileName=" + sourceFileName +
            ", fileFormat=" + fileFormat +
            ", receivedAsMediaType=" + receivedAsMediaType +
            ", mediaType=" + mediaType +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataFile dataFile = (DataFile) o;
    return Objects.equals(key, dataFile.key) &&
            Objects.equals(filePath, dataFile.filePath) &&
            Objects.equals(sourceFileName, dataFile.sourceFileName) &&
            Objects.equals(fileFormat, dataFile.fileFormat) &&
            Objects.equals(receivedAsMediaType, dataFile.receivedAsMediaType) &&
            Objects.equals(mediaType, dataFile.mediaType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            key,
            filePath,
            sourceFileName,
            fileFormat,
            receivedAsMediaType,
            mediaType);
  }

}
