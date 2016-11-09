package org.gbif.validation.ws;

import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

import com.sun.jersey.core.header.ContentDisposition;
import com.sun.jersey.multipart.file.DefaultMediaTypePredictor;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.fileupload.FileUploadBase;
import org.apache.commons.fileupload.util.LimitedInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible to manage files uploaded for validation.
 */
public class UploadedFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedFileManager.class);

  private static final String ZIP_CONTENT_TYPE = DefaultMediaTypePredictor.CommonMediaTypes.ZIP.getMediaType().toString();
  private static final String TXT_CONTENT_TYPE = DefaultMediaTypePredictor.CommonMediaTypes.TXT.getMediaType().toString();
  private static final String CSV_CONTENT_TYPE = "text/csv";
  private static final String TSV_CONTENT_TYPE = "text/tab-separated-values";

  private final static List<String> TABULAR_CONTENT_TYPE = Arrays.asList(TXT_CONTENT_TYPE, CSV_CONTENT_TYPE, TSV_CONTENT_TYPE);

  private final int FILE_DOWNLOAD_TIMEOUT_MS = 10000;

  private final String workingDirectory;
  private final long maxFileDownloadSize;

  /**
   *
   * @param workingDirectory
   * @param maxFileDownloadSize maximum size, in bytes, this manager is allowed to download from a URL.
   * @throws IOException
   */
  public UploadedFileManager(String workingDirectory, long maxFileDownloadSize) throws IOException {
    this.workingDirectory = workingDirectory;
    this.maxFileDownloadSize  = maxFileDownloadSize;

    File workingDirectoryFile = new File(workingDirectory);
    try {
      if(!workingDirectoryFile.exists()) {
        Files.createDirectory(new File(workingDirectory).toPath());
      }
    }
    catch (IOException ioEx){
      LOG.error("Can not create validation working directory: {}", workingDirectory);
      throw ioEx;
    }
  }

  /**
   * Warning, the inputStream will be closed after copy.
   *
   * @param filename
   * @param contentType
   * @param inputStream
   * @return
   * @throws IOException
   */
  public Optional<DataFileDescriptor> handleFileTransfer(String filename, String contentType, InputStream inputStream) throws IOException {
    java.nio.file.Path destinationFolder = generateRandomFolderPath();
    Files.createDirectory(destinationFolder);

    DataFileDescriptor dataFileDescriptor = new DataFileDescriptor();
    java.nio.file.Path uploadedResourcePath;

    try {
      //check if we have something to unzip
      if (ZIP_CONTENT_TYPE.equalsIgnoreCase(contentType)) {
        try {
          unzip(inputStream, destinationFolder);
          uploadedResourcePath = destinationFolder;
          //a little bit risky to assume that
          dataFileDescriptor.setFormat(FileFormat.DWCA);
        } catch (ArchiveException arEx) {
          LOG.error("Issue while unzipping data from {}.", arEx, filename);
          throw new IOException(arEx);
        }
      } else if (TABULAR_CONTENT_TYPE.contains(contentType)) {
        //preserve the extension (mostly for debugging)
        String fileExt = FilenameUtils.getExtension(filename);
        uploadedResourcePath = destinationFolder.resolve(UUID.randomUUID().toString() + "." + fileExt);

        Files.copy(inputStream, uploadedResourcePath);
        inputStream.close();
        dataFileDescriptor.setFormat(FileFormat.TABULAR);
      } else {
        LOG.warn("Unsupported file type: {}", contentType);
        return Optional.empty();
      }
    }
    catch (IOException ioEx) {
      LOG.warn("Deleting temporary content of {} after IOException.", filename);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      //propagate exception
      throw ioEx;
    }

    dataFileDescriptor.setSubmittedFile(filename);
    dataFileDescriptor.setUploadedResourcePath(uploadedResourcePath);
    return Optional.of(dataFileDescriptor);
  }

  public Optional<DataFileDescriptor> handleFileDownload(@Nullable String providedFilename,
                                                         @Nullable String providedContentType,
                                                         URL fileToDownload) throws IOException {

    String filename = StringUtils.trimToNull(providedFilename);
    String contentType = StringUtils.trimToNull(providedContentType);

    URLConnection urlConnection = fileToDownload.openConnection();
    urlConnection.setConnectTimeout(FILE_DOWNLOAD_TIMEOUT_MS);
    urlConnection.setReadTimeout(FILE_DOWNLOAD_TIMEOUT_MS);

    if(filename == null) {
      String contentDisposition = urlConnection.getHeaderField(FileUploadBase.CONTENT_DISPOSITION);

      if(StringUtils.isNotBlank(contentDisposition)) {
        try {
          ContentDisposition cd = new ContentDisposition(contentDisposition);
          filename = cd.getFileName();
        } catch (ParseException ignore) {
          ignore.printStackTrace();
        }
      }

      //if we still have no name for the file, take it from URL as last resort
      if(filename == null) {
        String tempFilename = FilenameUtils.getName(fileToDownload.toString());
        if(StringUtils.isNotBlank(tempFilename)) {
          filename = tempFilename;
        }
      }
    }

    if(contentType == null) {
      try {
        ContentType ct = ContentType.parse(urlConnection.getContentType());
        contentType = ct.getMimeType();
      }
      catch (org.apache.http.ParseException ignore){}
    }

    try (InputStream in = new FileDownloadLimitedInputStream(fileToDownload.openStream(), maxFileDownloadSize)) {
      return handleFileTransfer(filename, contentType, in);
    }
  }

  /**
   * Creates a new random path to be used when copying files.
   */
  java.nio.file.Path generateRandomFolderPath() {
    return Paths.get(workingDirectory, UUID.randomUUID().toString());
  }

  /**
   * Warning, this will close the input stream.
   * VISIBLE-FOR-TESTING
   *
   * @param zippedInputStream
   * @param destinationFile
   * @throws ArchiveException
   * @throws IOException
   */
  protected void unzip(InputStream zippedInputStream, java.nio.file.Path destinationFile) throws ArchiveException, IOException {

    try(ArchiveInputStream ais = new
            ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP, zippedInputStream)){
      Optional<ZipArchiveEntry> entry = Optional.ofNullable ((ZipArchiveEntry) ais.getNextEntry());
      while (entry.isPresent()) {
        if(entry.get().isDirectory()){
          Files.createDirectory(destinationFile.resolve(entry.get().getName()));
        }
        else {
          Files.copy(ais, destinationFile.resolve(entry.get().getName()));
        }
        entry = Optional.ofNullable ((ZipArchiveEntry) ais.getNextEntry());
      }
    }
  }

  /**
   * {@link LimitedInputStream} used to download external files for validation.
   */
  public static class FileDownloadLimitedInputStream extends LimitedInputStream {

    public FileDownloadLimitedInputStream(InputStream inputStream, long sizeMax) {
      super(inputStream, sizeMax);
    }

    protected void raiseError(long pSizeMax, long pCount) throws IOException {
      throw new FileDownloadSizeException(
              String.format("Download was rejected because its size (%s) exceeds the configured maximum (%s)",
                      new Object[]{Long.valueOf(pCount), Long.valueOf(pSizeMax)}));
    }
  }

  /**
   * {@link IOException} triggered when the limit set for download size is reached.
   */
  public static class FileDownloadSizeException extends IOException {
    public FileDownloadSizeException(String pMsg) {
      super(pMsg);
    }
  }

}
