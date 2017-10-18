package org.gbif.validation.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Method(s) to normalize files to ensure compatibility with other components.
 */
public class FileNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(FileNormalizer.class);

  public static final String END_LINE = "\n";
  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final Charset OUTPUT_FILE_CHARSET = StandardCharsets.UTF_8;

  /**
   * Write a new file after applying transformations on the source file.
   * Transformations includes: setting the endline characters, character encoding (UTF-8) and that the last line
   * includes a endline character.
   * TODO return object to be able to include the error message
   *
   * @param sourceFilePath
   * @param normalizedFilePath
   * @param sourceFilePathCharset optionally, the charset of the source file, otherwise UTF-8 will be used
   *
   * @return number of line written to the new file
   */
  public static int normalizeFile(Path sourceFilePath, Path normalizedFilePath,
                                  Charset sourceFilePathCharset) {
    Preconditions.checkArgument(!Files.isDirectory(sourceFilePath), "sourceFilePath must represent a file");
    Preconditions.checkArgument(!Files.isDirectory(normalizedFilePath), "normalizedFilePath must represent a file");

    final AtomicInteger numberOfLine = new AtomicInteger(0);
    try (Stream<String> lines = Files.lines(sourceFilePath,
            Optional.ofNullable(sourceFilePathCharset).orElse(DEFAULT_CHARSET));
         BufferedWriter writer = Files.newBufferedWriter(normalizedFilePath, OUTPUT_FILE_CHARSET)) {
      lines.forEach(line -> {
        try {
          writer.append(line);
          writer.append(END_LINE);
          numberOfLine.incrementAndGet();
        } catch (IOException ioEx) {
          LOG.error("Issue while writing to normalized file", ioEx);
        }
      });
    } catch (UncheckedIOException | IOException ioEx) {
      LOG.warn("Issue while reading " + sourceFilePath.toString(), ioEx);
      numberOfLine.set(0);
      try {
        Files.deleteIfExists(normalizedFilePath);
      } catch (IOException e) {
        LOG.warn("Issue while deleting incomplete file " + normalizedFilePath.toString(), e);
      }
    }
    return numberOfLine.get();
  }

  /**
   * Write new files after applying transformations on the source target (file(s) or file(s) within folder(s)) from the
   * specified folder.
   * This function "walks" inside the folder recursively.
   * See {@link #normalizeFile(Path, Path, Charset)}
   *
   * TODO this function doesn't handle quoted cells which means the numberOfLines may not be equals to the number of records.
   *
   * @param sourceTargetPath
   * @param destinationFolderPath
   * @param sourceTargetCharset   path should be relative to sourceTargetPath
   *
   * @return Map linking path to their line count. Paths are relative to sourceTargetPath.
   *
   * @throws IOException
   */
  public static Map<Path, Integer> normalizeTarget(Path sourceTargetPath, Path destinationFolderPath,
                                                   Map<Path, Charset> sourceTargetCharset)
          throws IOException {
    Path sourceFolderPath = Files.isDirectory(sourceTargetPath) ? sourceTargetPath : sourceTargetPath.getParent();
    Preconditions.checkArgument(sourceFolderPath != destinationFolderPath, "sourceFolderPath can NOT be the same as destinationFolderPath");
    Preconditions.checkArgument(Files.isDirectory(destinationFolderPath), "destinationFolderPath must represent a folder");

    Map<Path, Integer> linesPerFile = new HashMap<>();
    try (Stream<Path> paths = Files.isDirectory(sourceTargetPath) ? Files.walk(sourceFolderPath) :
            Arrays.asList(sourceTargetPath).stream()) {
      paths.forEach(filePath -> {
        if (Files.isRegularFile(filePath)) {
          Path destinationFile = destinationFolderPath.resolve(filePath.getFileName());
          Path relativePath = sourceFolderPath.relativize(filePath);
          int numberOfLines = FileNormalizer.normalizeFile(filePath, destinationFile,
                  Optional.ofNullable(sourceTargetCharset).map(charsetMap -> charsetMap.get(relativePath)).orElse(null));
          linesPerFile.put(relativePath, numberOfLines);
        }
      });
    }
    return linesPerFile;
  }

}
