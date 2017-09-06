package org.gbif.detect;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.apache.tika.Tika;

/**
 * Utility class to automatically detect the media types based on file or bytes.
 */
public class FileTypeDetector {

  private static final Tika TIKA = new Tika();

  private FileTypeDetector() {}

  /**
   *
   * @param filePath
   * @return detected media type
   * @throws IOException
   */
  public static String detectFormat(Path filePath) throws IOException {
    return TIKA.detect(filePath);
  }

  /**
   * detected media type
   * @param is
   * @return
   * @throws IOException
   */
  public static String detectFormat(InputStream is) throws IOException {
    return TIKA.detect(is);
  }
}
