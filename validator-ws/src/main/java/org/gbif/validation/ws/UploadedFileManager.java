package org.gbif.validation.ws;

import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import com.sun.jersey.multipart.file.DefaultMediaTypePredictor;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible to manage files uploaded for validation.
 */
public class UploadedFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedFileManager.class);

  private final String ZIP_CONTENT_TYPE = DefaultMediaTypePredictor.CommonMediaTypes.ZIP.getMediaType().toString();

  private final String workingDirectory;

  public UploadedFileManager(String workingDirectory) throws IOException {
    this.workingDirectory = workingDirectory;

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
  public DataFileDescriptor handleFileUpload(String filename, String contentType, InputStream inputStream) throws IOException {
    java.nio.file.Path destinationFolder = generateRandomFolderPath();
    Files.createDirectory(destinationFolder);

    DataFileDescriptor dataFileDescriptor = new DataFileDescriptor();

    java.nio.file.Path uploadedResourcePath = null;

    //check if we have something to unzip
    if(contentType.equalsIgnoreCase(ZIP_CONTENT_TYPE)) {
      try {
        unzip(inputStream, destinationFolder);
        uploadedResourcePath = destinationFolder;
        dataFileDescriptor.setFormat(FileFormat.DWCA);
      } catch (ArchiveException e) {
        LOG.error("Issue while unzipping data at {}", e, destinationFolder);
        //not sure this is the best way to handle that
        throw new IOException(e);
      }
    }
    else{ //copy a single file
      uploadedResourcePath = destinationFolder.resolve(UUID.randomUUID().toString()+".csv");
      Files.copy(inputStream, uploadedResourcePath, StandardCopyOption.REPLACE_EXISTING);
      inputStream.close();
      dataFileDescriptor.setFormat(FileFormat.TABULAR);
    }

    dataFileDescriptor.setSubmittedFile(filename);
    dataFileDescriptor.setUploadedResourcePath(uploadedResourcePath);
    return dataFileDescriptor;
  }

  /**
   * Creates a new random path to be used when copying files.
   */
  java.nio.file.Path generateRandomFolderPath() {
    return Paths.get(workingDirectory, UUID.randomUUID().toString());
  }

  /**
   * Warning, this will close the input stream.
   *
   * @param zippedInputStream
   * @param destinationFile
   * @throws ArchiveException
   * @throws IOException
   */
  protected void unzip(InputStream zippedInputStream, java.nio.file.Path destinationFile) throws ArchiveException, IOException {
    ArchiveInputStream ais = new
            ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP, zippedInputStream);
    ZipArchiveEntry entry = (ZipArchiveEntry) ais.getNextEntry();
    while (entry != null) {
      if(entry.isDirectory()){
        Files.createDirectory(destinationFile.resolve(entry.getName()));
      }
      else {
        Files.copy(ais, destinationFile.resolve(entry.getName()));
        entry = (ZipArchiveEntry) ais.getNextEntry();
      }
    }
    ais.close();
  }

}
