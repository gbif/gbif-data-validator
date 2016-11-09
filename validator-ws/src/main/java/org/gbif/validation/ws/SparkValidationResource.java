package org.gbif.validation.ws;

import org.gbif.utils.HttpUtil;
import org.gbif.validation.DataValidationClient;
import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.ws.conf.ValidationConfiguration;
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
import com.google.inject.Singleton;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.FormDataParam;
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
public class SparkValidationResource {

  private final ValidationConfiguration configuration;
  private final HttpUtil httpUtil;
  private final UploadedFileManager uploadedFileManager;

  @Inject(optional = true )
  private DataValidationClient dataValidationClient;

  private final Configuration hadoopConf;

  private static final Logger LOG = LoggerFactory.getLogger(SparkValidationResource.class);

  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";
  private static final String FILE_PARAM = "file";

  @Inject
  public SparkValidationResource(ValidationConfiguration configuration, ResourceEvaluationManager resourceEvaluationManager,
                                 HttpUtil httpUtil) throws IOException {
    this.configuration = configuration;
    this.httpUtil = httpUtil;
    uploadedFileManager = new UploadedFileManager(configuration.getWorkingDir());
    hadoopConf = buildHadoopConf();
    initUploadDirectory(configuration.getWorkingDir());
  }

  /**
   * Gets the path where files are uploaded. If it doesn't exist, the directory is created.
   */
  private static java.nio.file.Path initUploadDirectory(String uploadDir) throws IOException {
    java.nio.file.Path fileUploadDirectory = Paths.get(uploadDir).resolve(FILEUPLOAD_TMP_FOLDER);
    if(!fileUploadDirectory.toFile().exists()) {
      Files.createDirectory(fileUploadDirectory);
    }
    return fileUploadDirectory;
  }

  /**
   * Creates a Hadoop Configuration instance.
   */
  private static Configuration buildHadoopConf() {
    Configuration conf = new Configuration();
    conf.addResource("hdfs-site.xml");
    conf.addResource("core-site.xml");
    return conf;
  }


  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/spark")
  public ValidationResult validateFileOnSpark(@FormDataParam(FILE_PARAM) InputStream stream,
                                              @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                                              FormDataMultiPart formDataMultiPart) {
    DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart, header);
    URI dataFilePath = downloadFile(dataFileDescriptor, stream);
    return processFile(dataFilePath, dataFileDescriptor);
  }

  private URI downloadFile(DataFileDescriptor descriptor, InputStream stream) {
    if(descriptor.getSubmittedFile() != null) {
      try {
        if (descriptor.getSubmittedFile().startsWith("http")) {
          File downloadedFile = downloadHttpFile(new URL(descriptor.getSubmittedFile()));
          return moveToHdfs(downloadedFile, descriptor.getSubmittedFile()).toUri();
        }
        return copyToHdfs(stream,descriptor.getSubmittedFile()).toUri();
      } catch(IOException  ex){
        throw new WebApplicationException(ex, SC_BAD_REQUEST);
      }
    }
    throw new WebApplicationException(SC_BAD_REQUEST);
  }

  /**
   * Downloads a file from a HTTP(s) endpoint.
   */
  private File downloadHttpFile(URL fileUrl) throws IOException {
    java.nio.file.Path destinationFilePath = uploadedFileManager.generateRandomFolderPath().resolve(UUID.randomUUID().toString());
    File file = destinationFilePath.toFile();
    if (httpUtil.download(fileUrl, file).getStatusCode() == SC_OK) {
      return file;
    }
    throw new WebApplicationException(SC_BAD_REQUEST);
  }

  /**
   * Move a file to HDFS and deletes the file from the local file system.
   */
  private org.apache.hadoop.fs.Path moveToHdfs(File file, String fileName) throws IOException {
    org.apache.hadoop.fs.Path filePath = copyToHdfs(new FileInputStream(file),fileName);
    file.delete();
    return  filePath;
  }

  /**
   * Copies the stream content into a file in HDFS.
   */
  private org.apache.hadoop.fs.Path copyToHdfs(InputStream stream, String fileName) throws IOException {
    org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(configuration.getWorkingDir() + '/' + UUID.randomUUID() + '/' + fileName);
    try(OutputStream os = FileSystem.get(hadoopConf).create(filePath)) {
      IOUtils.copyBytes(stream, os, hadoopConf);
    }
    return filePath;
  }

  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(URI dataFileUri, DataFileDescriptor dataFileDescriptor)  {
    try {
      ScalaJobHandle<ValidationResultElement> jobHandle = (ScalaJobHandle<ValidationResultElement>)Await.ready(dataValidationClient.processDataFile(dataFileUri.getPath()), Duration.create(10000, TimeUnit.SECONDS));

      return ValidationResultBuilders.Builder.of(true, dataFileDescriptor.getSubmittedFile(), FileFormat.TABULAR,
                                                 ValidationProfile.GBIF_INDEXING_PROFILE)
                                             .withResourceResult(jobHandle.value().get().get()).build();
    } catch (Exception ex) {
      throw new WebApplicationException(ex, SC_INTERNAL_SERVER_ERROR);
    } finally {
      deleteFile(dataFileUri);
    }
  }

  /**
   * Deletes the uploaded file on HDFS.
   */
  private void deleteFile(URI fileUri) {
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
        fs.delete(new org.apache.hadoop.fs.Path(fileUri), false);
    } catch (Exception ex) {
      LOG.error("Error deleting file {}", fileUri, ex);
    }
  }

}
