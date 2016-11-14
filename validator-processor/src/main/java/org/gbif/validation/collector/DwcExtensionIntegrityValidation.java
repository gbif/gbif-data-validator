package org.gbif.validation.collector;

import org.gbif.validation.api.model.DataFileDescriptor;
import static  org.gbif.validation.util.FileBashUtilities.findInFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringEscapeUtils;

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
  public static List<String> collectUnlinkedExtensions(DataFileDescriptor coreDescriptor, int coreColumn,
                                                      DataFileDescriptor extDescriptor, int extColumn,
                                                      long maxSampleSize) throws IOException {

    try(Stream<String> lines = Files.lines(Paths.get(extDescriptor.getSubmittedFile()))) {

      return lines.skip(extDescriptor.isHasHeaders() ? 1 : 0)
                  .filter(line -> getColumnValue(line, extColumn, extDescriptor.getFieldsTerminatedBy().toString())
                                  .map(valueIsNotInFile(coreDescriptor, coreColumn)).orElse(false))
                  .limit(maxSampleSize)
                  .collect(Collectors.toList());
    }
  }

  /**
   * Validates if the String function parameter exists in any line[column] of the descriptor.getSubmittedFile.
   * It was created to maintain readability in the collectUnlinkedExtension method.
   */
  private static Function<String,Boolean> valueIsNotInFile(DataFileDescriptor descriptor, int column){
    return val -> {
        try {
          return findInFile(descriptor.getSubmittedFile(), val, column + 1, //bash uses 1-based indexes
                            StringEscapeUtils.escapeJava(descriptor.getFieldsTerminatedBy().toString())).length == 0;
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
    return column <= values.length ? Optional.of(values[column]) : Optional.empty();
  }

}
