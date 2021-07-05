package org.gbif.validator.ws.file;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class ZipUtil {

  //Files with name in the list will be ignored from a zip file
  protected static final List<String> FILE_EXCLUSION_LIST = Collections.singletonList(".DS_Store");

  //Folders with name in the list (from the root, not recursively) will be ignored from a zip file
  protected static final List<String> FOLDER_EXCLUSION_LIST = Collections.singletonList("__MACOSX");

  @SneakyThrows
  protected static Path unzip(Path dataFilePath, Path destinationFile) {
    try (ZipInputStream ais = new ZipInputStream(new FileInputStream(dataFilePath.toFile()))) {
      Path extractPath = destinationFile.resolve(FilenameUtils.getBaseName(dataFilePath.toFile().getName()));
      Files.createDirectories(extractPath);
      Optional<ZipEntry> entry = Optional.ofNullable(ais.getNextEntry());
      while (entry.isPresent()) {
        String entryName = entry.get().getName();
        if (!skipFolder(entryName)) {
          Path entryDestinationPath = new File(extractPath.toFile(), entryName).toPath();

          if (!entryDestinationPath.startsWith(extractPath)) {
            throw new RuntimeException("Unsupported entry. Entry is pointing to a path higher than allowed.");
          }

          if (entry.get().isDirectory()) {
            Files.createDirectory(entryDestinationPath);
          } else if (!skipFile(entryName)) {
            // ensure the folder structure exists otherwise create it.
            Files.createDirectories(entryDestinationPath.getParent());
            Files.copy(ais, entryDestinationPath);
          }
        }
        entry = Optional.ofNullable(ais.getNextEntry());
      }
      return extractPath;
    }
  }

  private static boolean skipFolder(String entryName) {
    return FOLDER_EXCLUSION_LIST.contains(StringUtils.substringBefore(entryName, "/"));
  }

  private static boolean skipFile(String entryName) {
    return FILE_EXCLUSION_LIST.contains(StringUtils.substringBefore(entryName, "/"));
  }
}
