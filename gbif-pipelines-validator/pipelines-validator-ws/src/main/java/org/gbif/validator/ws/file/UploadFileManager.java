package org.gbif.validator.ws.file;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import static org.gbif.validator.ws.file.MediaTypeAndFormatDetector.detectMediaType;
import static org.gbif.validator.ws.file.MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat;
import static org.gbif.validator.ws.file.SupportedMediaTypes.ZIP_CONTENT_TYPE;
import static org.gbif.validator.ws.file.ZipUtil.unzip;
import static org.gbif.validator.ws.file.DownloadFileManager.isAvailable;

/**
 * Class responsible to manage files uploaded for validation.
 * This class will unzip the file is required.
 */
public class UploadFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(UploadFileManager.class);

  @Data
  @Builder
  public static class AsyncDownloadResult {
    private final DataFile dataFile;
    private final CompletableFuture<File> downloadTask;
  }

  private final Path workingDirectory;
  private final Path storePath;

  private final DownloadFileManager downloadFileManager;

  public UploadFileManager(String workingDirectory, String storeDirectory, DownloadFileManager downloadFileManager) throws IOException {
    this.workingDirectory = Paths.get(workingDirectory);
    this.storePath = Paths.get(storeDirectory);
    this.downloadFileManager = downloadFileManager;
    createIfNotExists(this.workingDirectory);
    createIfNotExists(storePath);
  }

  @SneakyThrows
  public DataFile extractAndGetFileInfo(Path dataFilePath, Path destinationFolder, String fileName) {
    try {

      //check if we have something to unzip
      String detectedMediaType = detectMediaType(dataFilePath);
      Path finalPath = ZIP_CONTENT_TYPE.contains(detectedMediaType)? unzip(dataFilePath, destinationFolder) : dataFilePath;

      // from here we can decide to change the content type (e.g. zipped excel file)
      return evaluateMediaTypeAndFormat(finalPath, detectedMediaType)
              .map(mtf -> DataFile.newInstance(dataFilePath, fileName, mtf.getFileFormat(),
                                               detectedMediaType, mtf.getMediaType()))
              .orElseThrow(() -> new UnsupportedMediaTypeException("Unsupported file type: " + detectedMediaType));
    } catch (Exception ex) {
      LOG.warn("Deleting temporary content of {} after IOException.", fileName);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      throw ex;
    }
  }

  @SneakyThrows
  public DataFile uploadDataFile(MultipartFile multipartFile, String targetDirectory) {
    String fileName = multipartFile.getOriginalFilename();
    Path destinationFolder = getDestinationPath(targetDirectory);
    Path dataFilePath = destinationFolder.resolve(fileName);

    createIfNotExists(destinationFolder);

    //copy the file
    multipartFile.transferTo(dataFilePath.toFile());

    //check if we have something to unzip
    return extractAndGetFileInfo(dataFilePath, destinationFolder, fileName);
  }

  @SneakyThrows
  public AsyncDownloadResult downloadDataFile(String url, String targetDirectory,
                                   Consumer<DataFile> resultCallback,
                                   Consumer<Throwable> errorCallback) throws IOException {
    if (isAvailable(url)) {

      String fileName = getFileName(url);
      Path destinationFolder = getDestinationPath(targetDirectory);
      createIfNotExists(destinationFolder);
      Path dataFilePath = getDestinationPath(targetDirectory).resolve(fileName);
      return AsyncDownloadResult.builder()
              .dataFile(DataFile.builder()
                         .sourceFileName(fileName)
                         .filePath(dataFilePath)
                         .build())
              .downloadTask(downloadFileManager.downloadAsync(url,
                                                              dataFilePath,
                                                              file -> resultCallback.accept(extractAndGetFileInfo(dataFilePath,
                                                                                            destinationFolder,
                                                                                            fileName)),
                                                              errorCallback))
              .build();
    } else {
      throw new IllegalArgumentException("Url " + url + " is not reachable");
   }
  }

  @SneakyThrows
  private static String getFileName(String url) {
    return Paths.get(new URI(url).getPath()).getFileName().toString();
  }

  /**
   * Creates the directory(path) if it doesn't exist.
   */
  private static void  createIfNotExists(Path path) throws IOException {
    if (!path.toFile().exists()) {
      Files.createDirectories(path);
    }
  }

  /**
   * Creates a new random path to be used when copying files.
   */
  private Path getDestinationPath(String destinationDirectory) {
    return storePath.resolve(destinationDirectory);
  }

}
