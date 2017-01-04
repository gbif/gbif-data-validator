package org.gbif.validation.collector;

import org.gbif.validation.api.DataFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringEscapeUtils;

import static org.gbif.validation.util.FileBashUtilities.findInFile;

/**
 * This class validates data integrity between extension and core files in a DarwinCore archive.
 */
public class DwcExtensionIntegrityValidation {

  /**
   * Private constructor.
   */
  private DwcExtensionIntegrityValidation() {
    //empty constructor
  }

  /**
   * Collects values of extColumn that are not present in coreColumn. This is done scanning each line of extDescriptor,
   * extracting the column extColumn of extDescriptor.getSubmittedFile and matching it agains coreColumn of
   * coreDescriptor.getSubmittedFile.
   */
  public static List<String> collectUnlinkedExtensions(DataFile coreDescriptor, int coreColumn,
                                                       DataFile extDescriptor, int extColumn,
                                                       long maxSampleSize) throws IOException {

    try (Stream<String> lines = Files.lines(extDescriptor.getFilePath())) {

      //collect result of getColumnValue and not the entire line
      return lines.skip(extDescriptor.isHasHeaders() ? 1 : 0)  //skip the header, if it exists
              .map(line -> getColumnValue(line, extColumn, extDescriptor.getDelimiterChar().toString()))
              .filter(col -> col.map(valueIsNotInFile(coreDescriptor, coreColumn)).orElse(false))
              .limit(maxSampleSize)
              .collect(ArrayList::new, (list, el) -> list.add(el.get()),
                      (left, right) -> left.addAll(right));
    }
  }

  /**
   * Validates if the String function parameter exists in any line[column] of the descriptor.getSubmittedFile.
   * It was created to maintain readability in the collectUnlinkedExtension method.
   */
  private static Function<String,Boolean> valueIsNotInFile(DataFile descriptor, int column) {
    return val -> {
      try {
        return findInFile(descriptor.getFilePath().toString(), val, column + 1, //bash uses 1-based indexes
                          StringEscapeUtils.escapeJava(descriptor.getDelimiterChar().toString())).length == 0;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  /**
   * Gets line[column] if exists.
   */
  private static Optional<String> getColumnValue(String line, int column, String separator) {
    String[] values = line.split(separator);
    return column < values.length ? Optional.of(values[column]) : Optional.empty();
  }

}
