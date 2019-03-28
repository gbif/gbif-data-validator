package org.gbif.validation.ws.file;

import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.detect.MediaTypeAndFormatDetector;
import org.gbif.exception.UnsupportedMediaTypeException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.util.Cleanable;
import org.gbif.validation.ws.conf.ValidationWsConfiguration;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadBase;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.LimitedInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.detect.MediaTypeAndFormatDetector.detectMediaType;
import static org.gbif.validation.conf.SupportedMediaTypes.ZIP_CONTENT_TYPE;
import static org.gbif.validation.ws.utils.WebErrorUtils.errorResponse;

/**
 * Class responsible to manage files uploaded for validation.
 * This class will unzip the file is required.
 *
 */
public class UploadedFileManager implements Cleanable<UUID> {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedFileManager.class);

  //Files with name in the list will be ignored from a zip file
  protected static final List<String> FILE_EXCLUSION_LIST = Collections.singletonList(".DS_Store");
  //Folders with name in the list (from the root, not recursively) will be ignored from a zip file
  protected static final List<String> FOLDER_EXCLUSION_LIST = Collections.singletonList("__MACOSX");

  private static final Pattern FILENAME_PATTERN = Pattern.compile("filename[ ]*=[ ]*[\\S]+",Pattern.CASE_INSENSITIVE);
  private static final Pattern QUOTE_PATTERN = Pattern.compile("\"");

  private static final int FILE_DOWNLOAD_TIMEOUT_MS = 10000;
  private static final int MAX_SIZE_BEFORE_DISK_IN_BYTES = DiskFileItemFactory.DEFAULT_SIZE_THRESHOLD;
  private static final String DOWNLOAD_TMP_EXT = ".bin";
  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";

  private final Path workingDirectory;
  private final long maxFileTransferSizeInBytes;
  private final ServletFileUpload servletBasedFileUpload;

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
  protected static void unzip(InputStream zippedInputStream, Path destinationFile) throws ArchiveException, IOException {
    try (ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP,
            zippedInputStream)) {
      Optional<ZipArchiveEntry> entry = Optional.ofNullable((ZipArchiveEntry) ais.getNextEntry());
      while (entry.isPresent()) {
        String entryName = entry.get().getName();
        if (!FOLDER_EXCLUSION_LIST.contains(StringUtils.substringBefore(entryName, "/"))) {
          Path entryDestinationPath = new File(destinationFile.toFile(), entryName).toPath();

          if(!entryDestinationPath.startsWith(destinationFile)) {
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
        entry = Optional.ofNullable((ZipArchiveEntry) ais.getNextEntry());
      }
    }
  }

  /**
   * Try to extract the filename from a String extract from the HTTP headers.
   * VISIBLE-FOR-TESTING
   *
   * @param contentDisposition
   * @return
   */
  protected static Optional<String> parseContentDisposition(String contentDisposition) {
    Objects.requireNonNull(contentDisposition, "contentDisposition shall not be null");

    return Arrays.stream(contentDisposition.split(";"))
      .filter(el ->  FILENAME_PATTERN.matcher(el.trim()).matches())
      .map(el ->  QUOTE_PATTERN.matcher(el.split("=")[1].trim()).replaceAll(""))
      .findFirst();
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
    String fileExt = FilenameUtils.getExtension(filename);
    Path uploadedResourcePath = destinationFolder.resolve(UUID.randomUUID() + "." + fileExt);
    Files.copy(inputStream, uploadedResourcePath);
    inputStream.close();
    return uploadedResourcePath;
  }

  /**
   * if the provided file name is empty/null, this functions tries to gets it from the urlConnection.
   */
  private static String getFilename(String providedFilename, URLConnection urlConnection) {
    Optional<String> filename = Optional.ofNullable(StringUtils.trimToNull(providedFilename));
    if (!filename.isPresent()) {
      //Get it from the Content disposition
      String contentDisposition = urlConnection.getHeaderField(FileUploadBase.CONTENT_DISPOSITION);

      if(StringUtils.isNotBlank(contentDisposition)) {
        filename = parseContentDisposition(contentDisposition);
      }
      //if we still have no name for the file, take it from URL as last resort
      if (!filename.isPresent()) {
        //the connection name has the original URL
        String tempFilename = FilenameUtils.getName(urlConnection.getURL().toString());
        if(StringUtils.isNotBlank(tempFilename)) {
          filename = Optional.of(tempFilename);
        }
      }
    }
    return filename.get();
  }

  /**
   *
   * @param workingDirectory
   * @throws IOException
   */
  public UploadedFileManager(String workingDirectory, long maxFileTransferSizeInBytes) throws IOException {
    this.workingDirectory = Paths.get(workingDirectory, FILEUPLOAD_TMP_FOLDER);
    this.maxFileTransferSizeInBytes = maxFileTransferSizeInBytes;

    File workingDirectoryFile = this.workingDirectory.toFile();
    if(!workingDirectoryFile.exists()) {
      Files.createDirectories(this.workingDirectory);
    }

    DiskFileItemFactory diskFileItemFactory = new DiskFileItemFactory(MAX_SIZE_BEFORE_DISK_IN_BYTES,
                                                                      workingDirectoryFile);
    servletBasedFileUpload = new ServletFileUpload(diskFileItemFactory);
    servletBasedFileUpload.setFileSizeMax(maxFileTransferSizeInBytes);
  }

  /**
   * Handles the upload of data file from request parameters.
   */
  public Optional<DataFile> uploadDataFile(HttpServletRequest request) throws FileSizeException, UnsupportedMediaTypeException {
    Optional<String> uploadedFileName = Optional.empty();
    try {
      List<FileItem> uploadedContent = servletBasedFileUpload.parseRequest(request);
      Optional<FileItem> uploadFileInputStream = uploadedContent.stream()
        .filter(fileItem -> !fileItem.isFormField()
                            && ValidationWsConfiguration.FILE_POST_PARAM_NAME.equals(fileItem.getFieldName()))
        .findFirst();
      if (uploadFileInputStream.isPresent()) {
        FileItem uploadFileInputStreamVal = uploadFileInputStream.get();
        uploadedFileName = Optional.ofNullable(uploadFileInputStreamVal.getName());
        return handleFileTransfer(uploadFileInputStreamVal);
      }
    } catch (FileUploadException fileUploadEx) {
      LOG.error("FileUpload issue", fileUploadEx);
      throw new FileSizeException(fileUploadEx);
    } catch (IOException ioEx) {
      LOG.error("Can't handle uploaded file", ioEx);
      throw errorResponse(uploadedFileName.orElse(""), Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR);
    }
    throw errorResponse(Response.Status.BAD_REQUEST, ValidationErrorCode.UNSUPPORTED_FILE_FORMAT);
  }

  /**
   * Download a dataFile from a URL.
   * @param fileToDownload
   * @return
   * @throws IOException
   */
  public Optional<DataFile> downloadDataFile(URL fileToDownload) throws IOException, UnsupportedMediaTypeException {
    return downloadDataFile(null, fileToDownload);
  }

  /**
   * Warning, the inputStream will be closed after copy.
   *
   * @param filename
   * @param inputStream
   * @return a {@link DataFile} instance that represents the file that was transferred.
   * @throws IOException
   */
  protected Optional<DataFile> handleFileTransfer(String filename, InputStream inputStream)
    throws IOException, UnsupportedMediaTypeException {

    // "mark" needs to be supported in order to detect the media type by reading the first byte(s).
    InputStream inputStreamWithMarkSupport = inputStream.markSupported() ? inputStream : new BufferedInputStream(inputStream);

    UUID key = UUID.randomUUID();
    final Path destinationFolder = Files.createDirectory(getDestinationPath(key));
    final String detectedMediaType = detectMediaType(inputStreamWithMarkSupport, filename);

    Path dataFilePath;
    try {
      //check if we have something to unzip
      if (ZIP_CONTENT_TYPE.contains(detectedMediaType)) {
        try {
          unzip(inputStreamWithMarkSupport, destinationFolder);
          dataFilePath = determineDataFilePath(destinationFolder);
        } catch (ArchiveException arEx) {
          LOG.error("Issue while unzipping data from {}.", filename, arEx);
          throw new RuntimeException(arEx);
        }
      }
      else {
        dataFilePath = copyInputStream(destinationFolder, inputStreamWithMarkSupport, filename);
      }

      // from here we can decide to change the content type (e.g. zipped excel file)
      Optional<MediaTypeAndFormatDetector.MediaTypeAndFormat> mediaTypeAndFormat =
              MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(dataFilePath, detectedMediaType);

      if (!mediaTypeAndFormat.isPresent()) {
        throw new UnsupportedMediaTypeException("Unsupported file type: " + detectedMediaType);
      }

      return mediaTypeAndFormat
              .map(mtf -> DataFileFactory.newDataFile(key, dataFilePath, filename, mtf.getFileFormat(),
                      detectedMediaType, mtf.getMediaType()));
    } catch (IOException ioEx) {
      LOG.warn("Deleting temporary content of {} after IOException.", filename);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      //propagate exception
      throw ioEx;
    }
  }

  /**
   * Handles the file transfer from a FileItem.
   */
  private Optional<DataFile> handleFileTransfer(FileItem fileItem) throws IOException, UnsupportedMediaTypeException {
    return handleFileTransfer(fileItem.getName(), fileItem.getInputStream());
  }

  /**
   * Download a file from a URL. If providedFilename is null, the fileToDownload url is analyzed to extract them from
   * the headers.
   * The file will be completely downloaded into a temporary file before calling {@link #handleFileTransfer} to ensure
   * we have read the entire file.
   *
   */
  private Optional<DataFile> downloadDataFile(@Nullable String providedFilename,
                                              URL fileToDownload) throws IOException, UnsupportedMediaTypeException {
    UUID key = UUID.randomUUID();
    final Path destinationFile = workingDirectory.resolve(key.toString() + DOWNLOAD_TMP_EXT);
    try (InputStream in = new FileDownloadLimitedInputStream(fileToDownload.openStream(), maxFileTransferSizeInBytes);
         FileOutputStream out = new FileOutputStream(destinationFile.toFile());
    ) {

      // download the file entirely
      IOUtils.copy(in, out);

      Optional<DataFile> dataFile;
      try (FileInputStream downloadedInputStream = new FileInputStream(destinationFile.toFile())) {
        URLConnection urlConnection = fileToDownload.openConnection();
        urlConnection.setConnectTimeout(FILE_DOWNLOAD_TIMEOUT_MS);
        urlConnection.setReadTimeout(FILE_DOWNLOAD_TIMEOUT_MS);
        dataFile = handleFileTransfer(getFilename(providedFilename, urlConnection), downloadedInputStream);
      }

      Files.deleteIfExists(destinationFile);

      return dataFile;
    }
  }

  /**
   * This function is used to determine the final {@link Path} to use for the {@link DataFile}.
   * Currently, the main functionality is to skip the parent folder for Dwc-A in case the zip file
   * included a folder at its root.
   *
   * @param dataFilePath current {@link Path} of the {@link DataFile}
   *
   * @return
   */
  protected static Path determineDataFilePath(Path dataFilePath) {
    Objects.requireNonNull(dataFilePath, "dataFilePath shall be provided");
    File file = dataFilePath.toFile();
    Path filePath = dataFilePath;
    File[] content = file.listFiles();
    if (content.length == 1 && content[0].isDirectory()) {
      filePath = content[0].toPath();
    }
    return filePath;
  }

  /**
   * Creates a new random path to be used when copying files.
   */
  private Path getDestinationPath(UUID key) {
    return workingDirectory.resolve(key.toString());
  }

  @Override
  public void cleanByKey(UUID key) {
    Objects.requireNonNull(key, "key shall be provided");
    Path targetPath = workingDirectory.resolve(key.toString());
    LOG.warn("NOT trying to delete {} (in case we get a bug report)", targetPath);
    cleanUntil(LocalDateTime.now().minusMonths(2));
    //LOG.info("Trying to delete {}", targetPath);
    //try {
    //  FileUtils.deleteDirectory(targetPath.toFile());
    //} catch (IOException ioEx) {
    //  LOG.error("Failed to clean directory " + targetPath, ioEx);
    //}
  }

  @Override
  public void cleanUntil(LocalDateTime dateTimeLimit) {
    Objects.requireNonNull(dateTimeLimit, "dateTimeLimit shall be provided");
    Preconditions.checkArgument(dateTimeLimit.isBefore(LocalDateTime.now()),
            "dateTimeLimit can not be in the future");

    LOG.info("Deleting files uploaded before {}", dateTimeLimit);

    FileFilter tooOld = new AgeFileFilter(TemporalAccessorUtils.toDate(dateTimeLimit));
    for (File target : workingDirectory.toFile().listFiles(tooOld)) {
      LOG.info("Deleting old upload {}", target);
      try {
        if (target.isDirectory()) {
          FileUtils.deleteDirectory(target);
        } else {
          target.delete();
        }
      } catch (IOException ioEx) {
        LOG.error("Failed to delete " + target, ioEx);
      }
    }
  }

  /**
   * {@link LimitedInputStream} used to download external files for validation.
   */
  private static class FileDownloadLimitedInputStream extends LimitedInputStream {

    private FileDownloadLimitedInputStream(InputStream inputStream, long sizeMax) {
      super(inputStream, sizeMax);
    }

    @Override
    protected void raiseError(long pSizeMax, long pCount) throws IOException {
      throw new FileSizeException(
        String.format("Download was rejected because its size (%s) exceeds the configured maximum (%s)",
                      pCount, pSizeMax));
    }
  }

}
