package org.gbif.validator.ws.file;

import org.gbif.validator.api.FileFormat;

import java.nio.file.Path;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor(staticName = "newInstance")
public class DataFile {

  private final Path filePath;
  private final String sourceFileName;
  private FileFormat fileFormat;
  private String receivedAsMediaType;
  private String mediaType;

  public Long getSize() {
    if (filePath != null) {
      return filePath.toFile().length();
    }
    return null;
  }
}
