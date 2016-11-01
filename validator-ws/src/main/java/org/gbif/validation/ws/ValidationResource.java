package org.gbif.validation.ws;

import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.DataValidationClient;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.tabular.DataFileProcessorFactory;
import org.gbif.validation.util.FileBashUtilities;
import org.gbif.ws.server.provider.DataFileDescriptorProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.cloudera.livy.scalaapi.ScalaJobHandle;
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
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import static org.eclipse.jetty.server.Response.SC_BAD_REQUEST;
import static org.eclipse.jetty.server.Response.SC_INTERNAL_SERVER_ERROR;
import static org.eclipse.jetty.server.Response.SC_OK;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationResource {

  @Inject private ValidationConfiguration configuration;

  @Inject private DataFileProcessorFactory dataFileProcessorFactory;

  @Inject private HttpUtil httpUtil;

  @Inject(optional = true )private DataValidationClient dataValidationClient;

  private final Configuration hadoopConf;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final String FILE_PARAM = "file";

  public ValidationResource() {
    hadoopConf = new Configuration();
    hadoopConf.addResource("hdfs-site.xml");
    hadoopConf.addResource("core-site.xml");
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public ValidationResult validateFile(@FormDataParam(FILE_PARAM) InputStream stream,
                                       @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                                       FormDataMultiPart formDataMultiPart) {
      DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart, header);
      URI dataFilePath = downloadFile(dataFileDescriptor, stream, false);
      ValidationResult result = processFile(dataFilePath, dataFileDescriptor, false);

      return result;
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/spark")
  public ValidationResult validateFileOnSpark(@FormDataParam(FILE_PARAM) InputStream stream,
                                              @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                                              FormDataMultiPart formDataMultiPart) {
    DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart, header);
    URI dataFilePath = downloadFile(dataFileDescriptor, stream, true);
    ValidationResult result = processFile(dataFilePath, dataFileDescriptor, true);

    return result;
  }

  private URI downloadFile(DataFileDescriptor descriptor, InputStream stream, Boolean useHdfs) {
    if(descriptor.getSubmittedFile() != null) {
      try {
        return descriptor.getSubmittedFile().startsWith("http")? downloadHttpFile(new URL(descriptor.getSubmittedFile()),useHdfs) :
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
  private URI downloadHttpFile(URL fileUrl, boolean toHdfs) throws IOException {
    java.nio.file.Path destinationFilePath = downloadFilePath(Paths.get(fileUrl.getFile()).getFileName().toString());
    if (httpUtil.download(fileUrl, destinationFilePath.toFile()).getStatusCode() == SC_OK) {
      if(toHdfs) {
        copyToHdfs(destinationFilePath);
      }
      return destinationFilePath.toUri();
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
  private URI copyDataFile(InputStream stream,
                           DataFileDescriptor descriptor,
                           Boolean useHdfs) throws IOException {
    LOG.info("Uploading data file into {}", descriptor);
    if (!useHdfs) {
      java.nio.file.Path destinyFilePath = downloadFilePath(descriptor.getSubmittedFile());
      Files.createDirectory(destinyFilePath.getParent());
      Files.copy(stream, destinyFilePath, StandardCopyOption.REPLACE_EXISTING);
      return destinyFilePath.toUri();
    } else {
      return copyToHdfs(stream,descriptor.getSubmittedFile()).toUri();
    }

  }


  private org.apache.hadoop.fs.Path copyToHdfs(InputStream stream, String fileName) throws IOException {
    org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(configuration.getWorkingDir() + '/' + UUID.randomUUID() + '/' + fileName);
    try(FileSystem hdfs = FileSystem.get(hadoopConf); OutputStream os = hdfs.create(file)) {
      IOUtils.copyBytes(stream, os, hadoopConf);
    }
    return file;
  }

  private org.apache.hadoop.fs.Path copyToHdfs(java.nio.file.Path filePath) throws IOException {
    File file = filePath.toFile();
    return copyToHdfs(new FileInputStream(file),file.getName());
  }

  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(URI dataFileUri, DataFileDescriptor dataFileDescriptor, boolean useHdfs)  {
    try {
      if(useHdfs) {
        ScalaJobHandle<ValidationResult.RecordsValidationResourceResult> jobHandle = (ScalaJobHandle<ValidationResult.RecordsValidationResourceResult>)Await.ready(dataValidationClient.processDataFile(dataFileUri.getPath()), Duration.create(10000, TimeUnit.SECONDS));
        return ValidationResult.Builder.of(true, dataFileDescriptor.getSubmittedFile(),
                                           FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE)
          .withResourceResult(jobHandle.value().get().get()).build();

      } else {
        DataFile dataFile = new DataFile();
        java.nio.file.Path dataFilePath = Paths.get(dataFileUri);
        //set the original file name (mostly used to send it back in the response)
        dataFile.setSourceFileName(FilenameUtils.getName(dataFileDescriptor.getSubmittedFile()));
        dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
        dataFile.setNumOfLines(FileBashUtilities.countLines(dataFilePath.toFile().getAbsolutePath()));
        dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
        dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
        extractAndSetTabularFileMetadata(dataFilePath, dataFile);

        return dataFileProcessorFactory.create(dataFile).process(dataFile);
      }
    } catch (Exception ex) {
      throw new WebApplicationException(ex, SC_INTERNAL_SERVER_ERROR);
    } finally {
      deleteUri(dataFileUri, useHdfs);
    }

  }

  private void deleteUri(URI fileUri, boolean useHdfs) {
    try {
      if (useHdfs) {
        FileSystem.get(hadoopConf).delete(new org.apache.hadoop.fs.Path(fileUri), false);
      } else {
        Files.delete(Paths.get(fileUri));
      }
    } catch (Exception ex) {
      LOG.error("Error deleting file {}", fileUri, ex);
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
    //dataFile.loadHeaders();
  }
}
