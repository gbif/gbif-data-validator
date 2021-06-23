package org.gbif.validator.ws.file;

import org.gbif.validator.api.FileFormat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import static org.gbif.validator.ws.file.MediaTypeAndFormatDetector.detectMediaType;
import static org.gbif.validator.ws.file.SupportedMediaTypes.ZIP_CONTENT_TYPE;
import static org.gbif.validator.ws.config.ValidatorWsConfiguration.FILE_POST_PARAM_NAME;

/**
 * Class responsible to manage files uploaded for validation.
 * This class will unzip the file is required.
 *
 */
public class UploadedFileManager  {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedFileManager.class);

  //Files with name in the list will be ignored from a zip file
  protected static final List<String> FILE_EXCLUSION_LIST = Collections.singletonList(".DS_Store");
  //Folders with name in the list (from the root, not recursively) will be ignored from a zip file
  protected static final List<String> FOLDER_EXCLUSION_LIST = Collections.singletonList("__MACOSX");

  private final Path workingDirectory;
  private final Path storePath;
  private final long maxFileTransferSizeInBytes;

  /**
   * Warning, this will close the zippedInputStream {@link InputStream}.
   * VISIBLE-FOR-TESTING
   *
   * Very similar to CompressionUtil.decompressFile
   *
   * @param zippedInputStream
   * @param destinationFile
   *
   * @throws ArchiveException
   * @throws IOException
   */
  protected static void unzip(Path dataFilePath, Path destinationFile) throws ArchiveException, IOException {
    try (ZipInputStream ais = new ZipInputStream(new FileInputStream(dataFilePath.toFile()))) {
      Optional<ZipEntry> entry = Optional.ofNullable(ais.getNextEntry());
      while (entry.isPresent()) {
        String entryName = entry.get().getName();
        if (!FOLDER_EXCLUSION_LIST.contains(StringUtils.substringBefore(entryName, "/"))) {
          Path entryDestinationPath = new File(destinationFile.toFile(), entryName).toPath();

          if (!entryDestinationPath.startsWith(destinationFile)) {
            throw new ArchiveException("Unsupported entry. Entry is pointing to a path higher than allowed.");
          }

          if (entry.get().isDirectory()) {
            Files.createDirectory(entryDestinationPath);
          } else {
            if (!FILE_EXCLUSION_LIST.contains(StringUtils.substringAfterLast(entryName, "/"))) {
              // ensure the folder structure exists otherwise create it.
              Files.createDirectories(entryDestinationPath.getParent());
              Files.copy(ais, entryDestinationPath);
            }
          }
        }
        entry = Optional.ofNullable(ais.getNextEntry());
      }
    }
  }

  /**
   * Copy the provided {@link InputStream} into the provided destination folder using a random UUID and the
   * file extension extracted from filename.
   * @param destinationFolder
   * @param inputStream
   * @param filename
   * @return
   * @throws IOException
   */
  private static Path copyInputStream(Path destinationFolder, InputStream inputStream, String filename)
    throws IOException {
    Path outFile = destinationFolder.resolve(filename);
    try (FileOutputStream fileOutputStream = new FileOutputStream(outFile.toFile())) {
      IOUtils.copy(inputStream, fileOutputStream);
    }
    return outFile;
  }

  /**
   *
   * @param workingDirectory
   * @throws IOException
   */
  public UploadedFileManager(String workingDirectory, long maxFileTransferSizeInBytes, String storeDirectory) throws IOException {
    this.workingDirectory = Paths.get(workingDirectory);
    this.storePath = Paths.get(storeDirectory);
    this.maxFileTransferSizeInBytes = maxFileTransferSizeInBytes;

    File workingDirectoryFile = this.workingDirectory.toFile();
    if (!workingDirectoryFile.exists()) {
      Files.createDirectories(this.workingDirectory);
    }

    if (!storePath.toFile().exists()) {
      Files.createDirectories(storePath);
    }


  }


  public DataFile uploadDataFile(MultipartFile request, String targetDirectory) throws IOException {
    Path targetPath =getDestinationPath(targetDirectory).resolve(request.getOriginalFilename());
    request.transferTo(targetPath.toFile());
    return DataFile.builder().sourceFileName("adsd").fileFormat(FileFormat.DWCA).filePath(targetPath).receivedAsMediaType("pdf").build();
  }

  /**
   * Download a dataFile from a URL.
   * @param fileToDownload
   * @return
   * @throws IOException
   */
  public DataFile downloadDataFile(URL fileToDownload, String targetDirectory) throws IOException, UnsupportedMediaTypeException {
    return downloadDataFile(fileToDownload, targetDirectory);
  }

  /**
   * Warning, the inputStream will be closed after copy.
   *
   * @param filename
   * @param inputStream
   * @return a {@link DataFile} instance that represents the file that was transferred.
   * @throws IOException
   */
  protected DataFile handleFileTransfer(String filename, String targetDirectory, InputStream inputStream)
    throws IOException, UnsupportedMediaTypeException {

    // "mark" needs to be supported in order to detect the media type by reading the first byte(s).
    Path destinationFolder = Files.createDirectory(getDestinationPath(targetDirectory));

    try {
      //check if we have something to unzip
      Path dataFilePath = copyInputStream(destinationFolder, inputStream, filename);

      String detectedMediaType = detectMediaType(inputStream, filename);
      if (ZIP_CONTENT_TYPE.contains(detectedMediaType)) {
        try {
          unzip(dataFilePath, destinationFolder);
        } catch (ArchiveException arEx) {
          LOG.error("Issue while unzipping data from {}.", filename, arEx);
          throw new RuntimeException(arEx);
        }
      }

      // from here we can decide to change the content type (e.g. zipped excel file)

      return MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(dataFilePath, detectedMediaType)
              .map(mtf -> DataFile.newInstance(dataFilePath, filename, mtf.getFileFormat(),
                                               detectedMediaType, mtf.getMediaType()))
              .orElseThrow(() -> new UnsupportedMediaTypeException("Unsupported file type: " + detectedMediaType));
    } catch (IOException ioEx) {
      LOG.warn("Deleting temporary content of {} after IOException.", filename);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      //propagate exception
      throw ioEx;
    }
  }


  /**
   * Creates a new random path to be used when copying files.
   */
  private Path getDestinationPath(String destinationDirectory) {
    return storePath.resolve(destinationDirectory);
  }

}
