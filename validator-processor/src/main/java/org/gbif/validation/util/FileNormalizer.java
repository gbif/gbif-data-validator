package org.gbif.validation.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Method(s) to normalize files to ensure compatibility with other components.
 */
public class FileNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(FileNormalizer.class);

  public static final String END_LINE = "\n";
  public static final Charset OUTPUT_FILE_CHARSET =  StandardCharsets.UTF_8;

  /**
   * Write a new file after applying transformations on the source file.
   * Transformations includes: setting the endline characters, character encoding (UTF-8) and that the last line
   * includes a endline character.
   *
   * @param sourceFilePath
   * @param normalizedFilePath
   * @param sourceFilePathCharset optionally, the charset of the source file, otherwise UTF-8 will be used
   * @return number of line written to the new file
   */
  public static final long normalizeFile(Path sourceFilePath, Path normalizedFilePath,
                                         Optional<Charset> sourceFilePathCharset) {
    final AtomicLong numberOfLine = new AtomicLong(0l);
    try (Stream<String> lines = Files.lines(sourceFilePath, sourceFilePathCharset.orElse(StandardCharsets.UTF_8));
         BufferedWriter writer = Files.newBufferedWriter(normalizedFilePath, OUTPUT_FILE_CHARSET)) {
      lines.forEach(line -> {
        try {
          writer.append(line);
          writer.append(END_LINE);
          numberOfLine.incrementAndGet();
        } catch (IOException ioEx) {
          LOG.warn("Issue while writing to normalized file", ioEx);
        }
      });
    } catch (IOException ioEx) {
      LOG.warn("Issue while reading", ioEx);
    }
    return numberOfLine.get();
  }
}
