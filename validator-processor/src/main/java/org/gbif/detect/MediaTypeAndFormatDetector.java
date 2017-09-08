package org.gbif.detect;

import org.gbif.validation.api.vocabulary.FileFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tika.Tika;

import static org.gbif.validation.conf.SupportedMediaTypes.SPREADSHEET_CONTENT_TYPES;
import static org.gbif.validation.conf.SupportedMediaTypes.TABULAR_CONTENT_TYPES;
import static org.gbif.validation.conf.SupportedMediaTypes.ZIP_CONTENT_TYPE;

/**
 * Utility class to:
 * - automatically detect the media types based on file or bytes.
 * - decide org.gbif.validation.api.vocabulary.FileFormat based on media type
 */
public class MediaTypeAndFormatDetector {

  private static final Tika TIKA = new Tika();

  private MediaTypeAndFormatDetector() {}

  /**
   * @see org.apache.tika.detect.Detector
   * @param filePath
   * @return detected media type
   * @throws IOException
   */
  public static String detectMediaType(Path filePath) throws IOException {
    return TIKA.detect(filePath);
  }

  /**
   * detected media type
   * @see org.apache.tika.detect.Detector
   * @param is
   * @return
   * @throws IOException
   */
  public static String detectMediaType(InputStream is) throws IOException {
    return TIKA.detect(is);
  }


  /**
   * Given a {@link Path} to a file (or folder) and a original contentType this function
   * will check to reevaluate the contentType and return the matching {@link FileFormat}.
   * If a more specific contentType can not be found the original one will be return with the matching {@link FileFormat}.
   * @param dataFilePath shall point to data file or folder (not a zip file)
   * @param detectedContentType
   * @return
   */
  public static Optional<MediaTypeAndFormat> evaluateMediaTypeAndFormat(Path dataFilePath, String detectedContentType) throws IOException {
    Objects.requireNonNull(dataFilePath, "dataFilePath shall be provided");
    String currentDetectedContentType = detectedContentType;

    if (ZIP_CONTENT_TYPE.contains(detectedContentType)) {
      List<Path> content = Files.list(dataFilePath).collect(Collectors.toList());
      if (content.size() == 1) {
        currentDetectedContentType = MediaTypeAndFormatDetector.detectMediaType(content.get(0));
      } else {
        return Optional.of(new MediaTypeAndFormat(currentDetectedContentType, FileFormat.DWCA));
      }
    }

    if (TABULAR_CONTENT_TYPES.contains(currentDetectedContentType)) {
      return Optional.of(new MediaTypeAndFormat(currentDetectedContentType, FileFormat.TABULAR));
    } else if (SPREADSHEET_CONTENT_TYPES.contains(currentDetectedContentType)) {
      return Optional.of(new MediaTypeAndFormat(currentDetectedContentType, FileFormat.SPREADSHEET));
    }
    return Optional.empty();
  }

  /**
   * Simple holder for mediaType and fileFormat
   */
  public static class MediaTypeAndFormat {
    private final String mediaType;
    private final FileFormat fileFormat;

    public MediaTypeAndFormat(String mediaType, FileFormat fileFormat) {
      this.mediaType = mediaType;
      this.fileFormat = fileFormat;
    }

    public String getMediaType() {
      return mediaType;
    }

    public FileFormat getFileFormat() {
      return fileFormat;
    }
  }
}
