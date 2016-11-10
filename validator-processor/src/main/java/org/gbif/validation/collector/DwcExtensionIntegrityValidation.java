package org.gbif.validation.collector;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DwcExtensionIntegrityValidation {

  /**
   * Private constructor.
   */
  private DwcExtensionIntegrityValidation() {
    //empty constructor
  }

  public static List<String> collectUnlinkedExtension(DataFileDescriptor coreDescriptor, int coreColumn,
                                                      DataFileDescriptor extDescriptor, int extColumn,
                                                      long maxSampleSize, Path workingDir) throws IOException {

    Path coreSortedFile = sortFile(coreDescriptor, coreColumn, workingDir);
    Path extSortedFile = sortFile(extDescriptor, extColumn, workingDir);

    try(Stream<String> lines = Files.lines(extSortedFile)) {
      return lines.filter(line -> {
                                    try {
                                      return FileBashUtilities.findInFile(coreSortedFile.toString(), line).length == 0;
                                    } catch (IOException ex) {
                                      throw  new RuntimeException(ex);
                                    }
                                  })
                  .limit(maxSampleSize)
                  .collect(Collectors.toList());
    }
  }

  private static Path sortFile(DataFileDescriptor descriptor, int column, Path workingDir) throws IOException {
    FileUtils fileUtils = new FileUtils();
    Path sortedFile =  Files.createTempFile(workingDir, "val", ".tmp");
    fileUtils.sort(new File(descriptor.getSubmittedFile()), sortedFile.toFile(), descriptor.getEncoding().name(),
                   column, descriptor.getFieldsTerminatedBy().toString(), descriptor.getFieldsEnclosedBy(),
                   descriptor.getLinesTerminatedBy(), descriptor.isHasHeaders()? 1 : 0);
    return sortedFile;
  }

}
