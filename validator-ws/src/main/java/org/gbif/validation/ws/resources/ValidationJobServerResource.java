package org.gbif.validation.ws.resources;

import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.model.ValidationJobResponse;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.jobserver.JobServer;
import org.gbif.validation.ws.conf.ValidationConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

@Path("/jobserver")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationJobServerResource {

  private final ServletFileUpload servletBasedFileUpload;
  private final UploadedFileManager fileTransferManager;
  private final JobServer jobServer;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final int MAX_SIZE_BEFORE_DISK_IN_BYTES = DiskFileItemFactory.DEFAULT_SIZE_THRESHOLD;
  private static final long MAX_UPLOAD_SIZE_IN_BYTES = 1024*1024*100; //100 MB
  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";

  @Inject
  public ValidationJobServerResource(ValidationConfiguration configuration, JobServer<ValidationResult> jobServer)
    throws IOException {
    this.jobServer = jobServer;
    fileTransferManager = new UploadedFileManager(configuration.getWorkingDir(), MAX_UPLOAD_SIZE_IN_BYTES);

    //TODO clean on startup?
    java.nio.file.Path fileUploadDirectory = Paths.get(configuration.getWorkingDir()).resolve(FILEUPLOAD_TMP_FOLDER);
    if(!fileUploadDirectory.toFile().exists()) {
      Files.createDirectory(fileUploadDirectory);
    }

    DiskFileItemFactory diskFileItemFactory = new DiskFileItemFactory(MAX_SIZE_BEFORE_DISK_IN_BYTES, fileUploadDirectory.toFile());
    servletBasedFileUpload = new ServletFileUpload(diskFileItemFactory);
    servletBasedFileUpload.setFileSizeMax(MAX_UPLOAD_SIZE_IN_BYTES);
  }


  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/submit")
  public ValidationJobResponse onValidateFileAsync(@Context HttpServletRequest request) {

    Optional<String> uploadedFileName = Optional.empty();
    try {
      List<FileItem> uploadedContent = servletBasedFileUpload.parseRequest(request);
      Optional<FileItem> uploadFileInputStream = uploadedContent.stream().filter(
        fileItem -> !fileItem.isFormField() && WsValidationParams.FILE.getParam().equals(fileItem.getFieldName()))
        .findFirst();

      if(uploadFileInputStream.isPresent()) {
        FileItem uploadFileInputStreamVal = uploadFileInputStream.get();
        uploadedFileName = Optional.ofNullable(uploadFileInputStreamVal.getName());
        Optional<DataFile> dataFile = fileTransferManager.handleFileTransfer(uploadFileInputStreamVal);
        return dataFile.map(dataFileVal -> jobServer.submit(dataFileVal)).get();
      }
    }
    catch (FileUploadException fileUploadEx) {
      LOG.error("FileUpload issue", fileUploadEx);
      throw new WebApplicationException(fileUploadEx, SC_BAD_REQUEST);
    } catch (IOException ioEx) {
      LOG.error("Can't handle uploaded file", ioEx);
      throw errorResponse(uploadedFileName.orElse(""), Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR);
    }
    return ValidationJobResponse.FAILED_RESPONSE;
  }


  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/status/{jobid}")
  public ValidationJobResponse status(@PathParam("jobid") String jobid) {
    return jobServer.status(Long.valueOf(jobid));
  }


  /**
   * Prepare a {@link Response} object.
   *
   * @param uploadFileName
   * @param status
   * @param errorCode
   * @return
   */
  private static WebApplicationException errorResponse(String uploadFileName, Response.Status status, ValidationErrorCode errorCode) {
    Response.ResponseBuilder repBuilder = Response.status(status);
    repBuilder.entity(ValidationResultBuilders.Builder.withError(uploadFileName, null, errorCode).build());
    return new WebApplicationException(repBuilder.build());
  }
}
