package org.gbif.validation.ws;

import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.DataValidationClient;
import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.livy.scalaapi.ScalaJobHandle;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
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

  private final ValidationConfiguration configuration;
  private final ResourceEvaluationManager resourceEvaluationManager;
  private final HttpUtil httpUtil;

  @Inject(optional = true )
  private DataValidationClient dataValidationClient;

  private final Configuration hadoopConf;

  private final DiskFileItemFactory diskFileItemFactory;
  private final ServletFileUpload servletBasedFileUpload;
  private final UploadedFileManager uploadedFileManager;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final int MAX_SIZE_BEFORE_DISK_IN_BYTES = DiskFileItemFactory.DEFAULT_SIZE_THRESHOLD;
  private static final long MAX_UPLOAD_SIZE_IN_BYTES = 1024*1024*100; //100 MB
  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";
  private static final String FILE_PARAM = "file";

  @Inject
  public ValidationResource(ValidationConfiguration configuration, ResourceEvaluationManager resourceEvaluationManager,
                            HttpUtil httpUtil) throws IOException {
    this.configuration = configuration;
    this.resourceEvaluationManager = resourceEvaluationManager;
    this.httpUtil = httpUtil;

    hadoopConf = new Configuration();
    hadoopConf.addResource("hdfs-site.xml");
    hadoopConf.addResource("core-site.xml");

    uploadedFileManager = new UploadedFileManager(configuration.getWorkingDir());

    //TODO clean on startup?
    java.nio.file.Path fileUploadDirectory = Paths.get(configuration.getWorkingDir()).resolve(FILEUPLOAD_TMP_FOLDER);
    if(!fileUploadDirectory.toFile().exists()) {
      Files.createDirectory(fileUploadDirectory);
    }

    diskFileItemFactory = new DiskFileItemFactory(MAX_SIZE_BEFORE_DISK_IN_BYTES, fileUploadDirectory.toFile());
    servletBasedFileUpload = new ServletFileUpload(diskFileItemFactory);
    servletBasedFileUpload.setFileSizeMax(MAX_UPLOAD_SIZE_IN_BYTES);
  }
  
  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public ValidationResult onValidateFile(@Context HttpServletRequest request) {
    ValidationResult result = null;
    String uploadFileName = null;
    try {

      List<FileItem> uploadedContent = servletBasedFileUpload.parseRequest(request);
      Optional<FileItem> uploadFileInputStream = uploadedContent.stream().filter(
              fileItem -> !fileItem.isFormField() && FILE_PARAM.equals(fileItem.getFieldName()))
              .findFirst();

      if(uploadFileInputStream.isPresent()) {
        //TODO handle file download from URL
        Optional<DataFileDescriptor> dataFileDescriptor =
                uploadedFileManager.handleFileUpload(
                        uploadFileInputStream.get().getName(),
                        uploadFileInputStream.get().getContentType(),
                        uploadFileInputStream.get().getInputStream());
        uploadFileName = uploadFileInputStream.get().getName();
        if(dataFileDescriptor.isPresent()) {
          result = processFile(dataFileDescriptor.get().getUploadedResourcePath().toUri(),
                  dataFileDescriptor.get(), false);
        }
      }
    }
    catch (FileUploadException fileUploadEx) {
      LOG.error("FileUpload issue", fileUploadEx);
      throw new WebApplicationException(fileUploadEx, SC_BAD_REQUEST);
    } catch (IOException ioEx) {
      LOG.error("Can't handle uploaded file", ioEx);
      throw new WebApplicationException(buildErrorResponse(uploadFileName,
              Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR));
    }

    //it no error it thrown and we do not have result, we assume we can not handle
    //this file format
    if(result == null){
      throw new WebApplicationException(buildErrorResponse(uploadFileName,
              Response.Status.BAD_REQUEST, ValidationErrorCode.UNSUPPORTED_FILE_FORMAT));
    }
    return result;
  }

  /**
   * Prepare a {@link Response} object.
   *
   * @param uploadFileName
   * @param status
   * @param errorCode
   * @return
   */
  private Response buildErrorResponse(String uploadFileName, Response.Status status, ValidationErrorCode errorCode) {
    Response.ResponseBuilder repBuilder = Response.status(status);
    repBuilder.entity(ValidationResultBuilders.Builder.withError(uploadFileName, null, errorCode).build());
    return repBuilder.build();
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
    java.nio.file.Path destinationFilePath = uploadedFileManager.generateRandomFolderPath().resolve(UUID.randomUUID().toString());
    if (httpUtil.download(fileUrl, destinationFilePath.toFile()).getStatusCode() == SC_OK) {
      if(toHdfs) {
        copyToHdfs(destinationFilePath);
      }
      return destinationFilePath.toUri();
    }
    throw new WebApplicationException(SC_BAD_REQUEST);

  }


  /**
   * Copies the input stream into a temporary directory.
   */
  private URI copyDataFile(InputStream stream,
                           DataFileDescriptor descriptor,
                           Boolean useHdfs) throws IOException {
    LOG.info("Uploading data file into {}", descriptor);
    if (!useHdfs) {
      java.nio.file.Path destinyFilePath = uploadedFileManager.generateRandomFolderPath().resolve(UUID.randomUUID().toString());
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
        ScalaJobHandle<ValidationResultElement> jobHandle = (ScalaJobHandle<ValidationResultElement>)Await.ready(dataValidationClient.processDataFile(dataFileUri.getPath()), Duration.create(10000, TimeUnit.SECONDS));
        return ValidationResultBuilders.Builder.of(true, dataFileDescriptor.getSubmittedFile(),
                FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE)
          .withResourceResult(jobHandle.value().get().get()).build();

      } else {
        DataFile dataFile = new DataFile();
        java.nio.file.Path dataFilePath = Paths.get(dataFileUri);
        //set the original file name (mostly used to send it back in the response)
        dataFile.setSourceFileName(FilenameUtils.getName(dataFileDescriptor.getSubmittedFile()));
        dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
        dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
        dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
        dataFile.setFileFormat(dataFileDescriptor.getFormat());
        extractAndSetTabularFileMetadata(dataFilePath, dataFile);

        return resourceEvaluationManager.evaluate(dataFile);
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
  }
}
