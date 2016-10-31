package org.gbif.validation.ws;

import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.DataValidationClient;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.tabular.OccurrenceDataFileProcessorFactory;
import org.gbif.validation.util.FileBashUtilities;
import org.gbif.ws.server.provider.DataFileDescriptorProvider;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.FormDataParam;
import com.sun.jersey.spi.resource.Singleton;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.server.Response.SC_BAD_REQUEST;
import static org.eclipse.jetty.server.Response.SC_INTERNAL_SERVER_ERROR;
import static org.eclipse.jetty.server.Response.SC_OK;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationResource {

  @Inject private ValidationConfiguration configuration;

  @Inject private OccurrenceDataFileProcessorFactory dataFileProcessorFactory;

  @Inject private HttpUtil httpUtil;

  @Inject(optional = true )private DataValidationClient dataValidationClient;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final String FILE_PARAM = "file";

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public ValidationResult validateFile(@FormDataParam(FILE_PARAM) InputStream stream,
                                       @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                                       FormDataMultiPart formDataMultiPart) {
      DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart, header);
      java.nio.file.Path dataFilePath = downloadFile(dataFileDescriptor, stream, false);
      ValidationResult result = processFile(dataFilePath, dataFileDescriptor);
      //deletes the downloaded file
      dataFilePath.toFile().delete();
      return result;
  }

  private java.nio.file.Path downloadFile(DataFileDescriptor descriptor, InputStream stream, Boolean useHdfs) {
    if(descriptor.getFile() != null) {
      try {
        return descriptor.getFile().startsWith("http")? downloadHttpFile(new URL(descriptor.getFile()),useHdfs) :
                                                          copyDataFile(stream,descriptor, useHdfs);
      } catch(IOException  ex){
        throw new WebApplicationException(ex, SC_BAD_REQUEST);
      }
    }
    throw new WebApplicationException(SC_BAD_REQUEST);
  }

  /**
   * Downloads a file from a HTTP(s) endpoint.
   */
  private java.nio.file.Path downloadHttpFile(URL fileUrl, boolean toHdfs) throws IOException {
    java.nio.file.Path destinationFilePath = downloadFilePath(Paths.get(fileUrl.getFile()).getFileName().toString());
    if (httpUtil.download(fileUrl, destinationFilePath.toFile()).getStatusCode() == SC_OK) {
      if(toHdfs) {
        copyToHdfs(destinationFilePath);
      }
      return destinationFilePath;
    }
    throw new WebApplicationException(SC_BAD_REQUEST);

  }

  /**
   * Creates a new random path to be used when copying files.
   */
  private java.nio.file.Path downloadFilePath(String fileName) {
    return Paths.get(configuration.getWorkingDir(), UUID.randomUUID().toString(),fileName);
  }


  /**
   * Copies the input stream into a temporary directory.
   */
  private java.nio.file.Path copyDataFile(InputStream stream,
                                          DataFileDescriptor descriptor,
                                          Boolean useHdfs) throws IOException {
    LOG.info("Uploading data file into {}", descriptor);
    if (!useHdfs) {
      java.nio.file.Path destinyFilePath = downloadFilePath(descriptor.getFile());
      Files.createDirectory(destinyFilePath.getParent());
      Files.copy(stream, destinyFilePath, StandardCopyOption.REPLACE_EXISTING);
      return destinyFilePath;
    } else {
      return Paths.get(copyToHdfs(stream,descriptor.getFile()).toUri());
    }

  }


  private org.apache.hadoop.fs.Path copyToHdfs(InputStream stream, String fileName) throws IOException {
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource("core-site.xml");
    org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(configuration.getWorkingDir() + UUID.randomUUID() + "/" + fileName);
    try(FileSystem hdfs = FileSystem.get(hadoopConf); OutputStream os = hdfs.create(file)) {
      IOUtils.copyBytes(stream, os, hadoopConf);
    }
    return file;
  }

  private org.apache.hadoop.fs.Path copyToHdfs(java.nio.file.Path filePath) throws IOException {
    Configuration configuration = new Configuration();
    configuration.addResource("core-site.xml");
    org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("/datavalidation/" + UUID.randomUUID() + "/" + filePath.getFileName());
    try(FileSystem hdfs = FileSystem.get(configuration); OutputStream os = hdfs.create(file);
        FileInputStream inputFile = new FileInputStream(filePath.toFile())) {
      IOUtils.copyBytes(inputFile, os, configuration);
    }
    return file;
  }

  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(java.nio.file.Path dataFilePath, DataFileDescriptor dataFileDescriptor)  {
    try {
      DataFile dataFile = new DataFile();
      //set the original file name (mostly used to send it back in the response)
      dataFile.setSourceFileName(FilenameUtils.getName(dataFileDescriptor.getFile()));
      dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
      dataFile.setNumOfLines(FileBashUtilities.countLines(dataFilePath.toFile().getAbsolutePath()));
      dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
      dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
      extractAndSetTabularFileMetadata(dataFilePath, dataFile);

      return dataFileProcessorFactory.create(dataFile).process(dataFile);
    } catch (IOException ex) {
      //deletes the file in case of error
      dataFilePath.toFile().delete();
      throw new WebApplicationException(ex, SC_INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * TODO move to validator-processor
   * Method responsible to extract metadata (and headers) from the file identified by dataFilePath.
   *
   * @param dataFilePath location of the file
   * @param dataFile this object will be updated directly
   */
  private void extractAndSetTabularFileMetadata(java.nio.file.Path dataFilePath, DataFile dataFile) {
    //TODO make use of CharsetDetection.detectEncoding(source, 16384);
    if(dataFile.getDelimiterChar() == null) {
      try {
        CSVReaderFactory.CSVMetadata metadata = CSVReaderFactory.extractCsvMetadata(dataFilePath.toFile(), "UTF-8");
        if (metadata.getDelimiter().length() == 1) {
          dataFile.setDelimiterChar(metadata.getDelimiter().charAt(0));
        } else {
          throw new UnkownDelimitersException(metadata.getDelimiter() + "{} is a non supported delimiter");
        }
      } catch (UnkownDelimitersException udEx) {
        LOG.warn("Can not extractCsvMetadata of file {}", dataFilePath);
        throw new WebApplicationException(SC_BAD_REQUEST);
      }
    }
    //TODO fix this method to not load header if dataFileDescriptor.isHasHeaders() returns false
    dataFile.loadHeaders();
  }
}
