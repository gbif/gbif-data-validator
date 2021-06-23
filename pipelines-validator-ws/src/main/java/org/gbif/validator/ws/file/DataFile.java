package org.gbif.validator.ws.file;

import org.gbif.validator.api.FileFormat;

import java.nio.file.Path;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@Builder
@RequiredArgsConstructor(staticName = "newInstance")
public class DataFile {

  private final Path filePath;
  private final String sourceFileName;
  private final FileFormat fileFormat;
  private final String receivedAsMediaType;
  private final String mediaType;

}
