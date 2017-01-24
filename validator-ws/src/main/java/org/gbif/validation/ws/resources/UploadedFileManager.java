package org.gbif.validation.ws.resources;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.ws.conf.ValidationConfiguration;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.multipart.file.DefaultMediaTypePredictor.CommonMediaTypes;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.ws.utils.WebErrorUtils.errorResponse;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * Class responsible to manage files uploaded for validation.
 * This class will unzip the file is required.
 *
 */
public class UploadedFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedFileManager.class);

  private static final String ZIP_CONTENT_TYPE = CommonMediaTypes.ZIP.getMediaType().toString();

  private static final Pattern FILENAME_PATTERN = Pattern.compile("filename[ ]*=[ ]*[\\S]+",Pattern.CASE_INSENSITIVE);
  private static final Pattern QUOTE_PATTERN = Pattern.compile("\"");

  private static final List<String> TABULAR_CONTENT_TYPES = Arrays.asList(MediaType.TEXT_PLAIN,
                                                                          ExtraMediaTypes.TEXT_CSV,
                                                                          ExtraMediaTypes.TEXT_TSV);

  private static final List<String> SPREADSHEET_CONTENT_TYPES = Arrays.asList(
    ExtraMediaTypes.APPLICATION_EXCEL,
    ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET,
    ExtraMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET);

  private static final int FILE_DOWNLOAD_TIMEOUT_MS = 10000;

  private static final int MAX_SIZE_BEFORE_DISK_IN_BYTES = DiskFileItemFactory.DEFAULT_SIZE_THRESHOLD;

  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";

  private static final long MAX_UPLOAD_SIZE_IN_BYTES = 1024*1024*100; //100 MB

  private final Path workingDirectory;
  private final ServletFileUpload servletBasedFileUpload;

  /**
   * Warning, this will close the input stream.
   * VISIBLE-FOR-TESTING
   *
   * @param zippedInputStream
   * @param destinationFile
   * @throws ArchiveException
   * @throws IOException
   */
  protected static void unzip(InputStream zippedInputStream, Path destinationFile) throws ArchiveException, IOException {

    try (ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP,
                                                                                      zippedInputStream)) {
      Optional<ZipArchiveEntry> entry = Optional.ofNullable((ZipArchiveEntry)ais.getNextEntry());
      while (entry.isPresent()) {
        if (entry.get().isDirectory()) {
          Files.createDirectory(destinationFile.resolve(entry.get().getName()));
        } else {
          Files.copy(ais, destinationFile.resolve(entry.get().getName()));
        }
        entry = Optional.ofNullable((ZipArchiveEntry)ais.getNextEntry());
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
   * if the provided file name is empty/null, this functions treis to gets it from the urlConnection.
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
          filename = Optional.ofNullable(tempFilename);
        }
      }
    }
    return filename.get();
  }

  /**
   * If the providedContentType is null, the urlConnection is analyzed to get its content type.
   */
  private static String getContentType(String providedContentType, URLConnection urlConnection) {
    String contentType = StringUtils.trimToNull(providedContentType);
    if (StringUtils.isEmpty(contentType)) {
      try {
        return ContentType.parse(urlConnection.getContentType()).getMimeType();
      } catch (ParseException|UnsupportedCharsetException ignore){}
    }
    return contentType;
  }

  /**
   *
   * @param workingDirectory
   * @throws IOException
   */
  public UploadedFileManager(String workingDirectory) throws IOException {
    this.workingDirectory =  Paths.get(workingDirectory,FILEUPLOAD_TMP_FOLDER);

    File workingDirectoryFile = this.workingDirectory.toFile();
    if(!workingDirectoryFile.exists()) {
      Files.createDirectory(this.workingDirectory);
    }

    DiskFileItemFactory diskFileItemFactory = new DiskFileItemFactory(MAX_SIZE_BEFORE_DISK_IN_BYTES,
                                                                      workingDirectoryFile);
    servletBasedFileUpload = new ServletFileUpload(diskFileItemFactory);
    servletBasedFileUpload.setFileSizeMax(MAX_UPLOAD_SIZE_IN_BYTES);
  }

  /**
   * Handles the upload of data file from request parameters.
   */
  public Optional<DataFile> uploadDataFile(HttpServletRequest request) {
    Optional<String> uploadedFileName = Optional.empty();
    try {
      List<FileItem> uploadedContent = servletBasedFileUpload.parseRequest(request);
      Optional<FileItem> uploadFileInputStream = uploadedContent.stream()
        .filter(fileItem -> !fileItem.isFormField()
                            && ValidationConfiguration.FILE_POST_PARAM_NAME.equals(fileItem.getFieldName()))
        .findFirst();
      if (uploadFileInputStream.isPresent()) {
        FileItem uploadFileInputStreamVal = uploadFileInputStream.get();
        uploadedFileName = Optional.ofNullable(uploadFileInputStreamVal.getName());
        return handleFileTransfer(uploadFileInputStreamVal);
      }
    } catch (FileUploadException fileUploadEx) {
      LOG.error("FileUpload issue", fileUploadEx);
      throw new WebApplicationException(fileUploadEx, SC_BAD_REQUEST);
    } catch (IOException ioEx) {
      LOG.error("Can't handle uploaded file", ioEx);
      throw errorResponse(uploadedFileName.orElse(""), Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR);
    }
    throw errorResponse(Response.Status.BAD_REQUEST, ValidationErrorCode.UNSUPPORTED_FILE_FORMAT);
  }

  public Optional<DataFile> uploadDataFile(URL fileToDownload) throws IOException {
    return uploadDataFile(null, null, fileToDownload);
  }

  /**
   * Warning, the inputStream will be closed after copy.
   *
   * @param filename
   * @param contentType
   * @param inputStream
   * @return a {@link DataFile} instance that represents the file that was transferred.
   * @throws IOException
   */
  private Optional<DataFile> handleFileTransfer(String filename, String contentType, InputStream inputStream)
    throws IOException {

    Path destinationFolder = Files.createDirectory(generateRandomFolderPath());

    DataFile transferredDataFile;
    try {
      //check if we have something to unzip
      if (ZIP_CONTENT_TYPE.equalsIgnoreCase(contentType)) {
        try {
          unzip(inputStream, destinationFolder);

          //a little bit risky to assume that
          transferredDataFile = DataFileFactory.newDataFile(destinationFolder,
                  filename, FileFormat.DWCA, contentType);
        } catch (ArchiveException arEx) {
          LOG.error("Issue while unzipping data from {}.", filename, arEx);
          throw new RuntimeException(arEx);
        }
      } else if (TABULAR_CONTENT_TYPES.contains(contentType)) {
        transferredDataFile = DataFileFactory.newDataFile(copyInputStream(destinationFolder, inputStream, filename),
                filename, FileFormat.TABULAR, contentType);
      } else if (SPREADSHEET_CONTENT_TYPES.contains(contentType)) {
        transferredDataFile = DataFileFactory.newDataFile(copyInputStream(destinationFolder, inputStream, filename),
                filename, FileFormat.SPREADSHEET, contentType);
      } else {
        LOG.warn("Unsupported file type: {}", contentType);
        return Optional.empty();
      }
    } catch (IOException ioEx) {
      LOG.warn("Deleting temporary content of {} after IOException.", filename);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      //propagate exception
      throw ioEx;
    }

    return Optional.of(transferredDataFile);
  }

  /**
   * Handles the file transfer from a FileItem.
   */
  private Optional<DataFile> handleFileTransfer(FileItem fileItem) throws IOException {
    return handleFileTransfer(fileItem.getName(), fileItem.getContentType(), fileItem.getInputStream());
  }

  /**
   * Uploads a file using two optional parameters for file name and content type.
   * If any of those parameters are null, the fileToDownload url is analized to extract them.
   */
  private Optional<DataFile> uploadDataFile(@Nullable String providedFilename,
                                            @Nullable String providedContentType,
                                            URL fileToDownload) throws IOException {

    try (InputStream in = new FileDownloadLimitedInputStream(fileToDownload.openStream(), MAX_UPLOAD_SIZE_IN_BYTES)) {
      URLConnection urlConnection = fileToDownload.openConnection();
      urlConnection.setConnectTimeout(FILE_DOWNLOAD_TIMEOUT_MS);
      urlConnection.setReadTimeout(FILE_DOWNLOAD_TIMEOUT_MS);
      return handleFileTransfer(getFilename(providedFilename, urlConnection),
                                getContentType(providedContentType,urlConnection), in);
    }
  }

  /**
   * Creates a new random path to be used when copying files.
   */
  private Path generateRandomFolderPath() {
    return workingDirectory.resolve(UUID.randomUUID().toString());
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
      throw new FileDownloadSizeException(
        String.format("Download was rejected because its size (%s) exceeds the configured maximum (%s)",
                      pCount, pSizeMax));
    }
  }

  /**
   * {@link IOException} triggered when the limit set for download size is reached.
   */
  private static class FileDownloadSizeException extends IOException {
    private FileDownloadSizeException(String pMsg) {
      super(pMsg);
    }
  }

}
