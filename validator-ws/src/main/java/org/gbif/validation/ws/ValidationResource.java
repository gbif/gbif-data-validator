package org.gbif.validation.ws;

import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.DataFileDescriptor;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.ws.conf.ValidationConfiguration;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.server.Response.SC_BAD_REQUEST;
import static org.eclipse.jetty.server.Response.SC_INTERNAL_SERVER_ERROR;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationResource {

  private final ResourceEvaluationManager resourceEvaluationManager;

  private final ServletFileUpload servletBasedFileUpload;
  private final UploadedFileManager fileTransferManager;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final int MAX_SIZE_BEFORE_DISK_IN_BYTES = DiskFileItemFactory.DEFAULT_SIZE_THRESHOLD;
  private static final long MAX_UPLOAD_SIZE_IN_BYTES = 1024*1024*100; //100 MB
  private static final String FILEUPLOAD_TMP_FOLDER = "fileupload";


  @Inject
  public ValidationResource(ValidationConfiguration configuration, ResourceEvaluationManager resourceEvaluationManager)
    throws IOException {
    this.resourceEvaluationManager = resourceEvaluationManager;

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
  @Path("/file")
  public ValidationResult onValidateFile(@Context HttpServletRequest request) {
    ValidationResult result = null;
    String uploadFileName = null;
    try {

      List<FileItem> uploadedContent = servletBasedFileUpload.parseRequest(request);
      Optional<FileItem> uploadFileInputStream = uploadedContent.stream().filter(
              fileItem -> !fileItem.isFormField() && WsValidationParams.FILE.getParam().equals(fileItem.getFieldName()))
              .findFirst();

      if(uploadFileInputStream.isPresent()) {
        Optional<DataFileDescriptor> dataFileDescriptor = fileTransferManager.handleFileTransfer(
                        uploadFileInputStream.get().getName(),
                        uploadFileInputStream.get().getContentType(),
                        uploadFileInputStream.get().getInputStream());
        uploadFileName = uploadFileInputStream.get().getName();
        if(dataFileDescriptor.isPresent()) {
          result = processFile(dataFileDescriptor.get().getUploadedResourcePath(), dataFileDescriptor.get());
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

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/url")
  public ValidationResult onValidateFile(@QueryParam("fileUrl") String fileURL) {
    ValidationResult result = null;
    try {
      Optional<DataFileDescriptor> dataFileDescriptor =
              fileTransferManager.handleFileDownload(null, null, new URL(fileURL));
      if(dataFileDescriptor.isPresent()) {
        result = processFile(dataFileDescriptor.get().getUploadedResourcePath(),
                dataFileDescriptor.get());
      }
    } catch (IOException ioEx) {
      LOG.error("Can't handle file download from {}", ioEx, fileURL);
      throw new WebApplicationException(buildErrorResponse(fileURL.toString(),
              Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR));
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
  private static Response buildErrorResponse(String uploadFileName, Response.Status status, ValidationErrorCode errorCode) {
    Response.ResponseBuilder repBuilder = Response.status(status);
    repBuilder.entity(ValidationResultBuilders.Builder.withError(uploadFileName, null, errorCode).build());
    return repBuilder.build();
  }

  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(java.nio.file.Path dataFilePath, DataFileDescriptor dataFileDescriptor)  {
    try {
        DataFile dataFile = new DataFile();
        //set the original file name (mostly used to send it back in the response)
        dataFile.setSourceFileName(FilenameUtils.getName(dataFileDescriptor.getSubmittedFile()));
        dataFile.setFilePath(dataFilePath.toAbsolutePath());
        dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
        dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
        dataFile.setFileFormat(dataFileDescriptor.getFormat());
        extractAndSetTabularFileMetadata(dataFilePath, dataFile);
        return resourceEvaluationManager.evaluate(dataFile);
    } catch (Exception ex) {
      throw new WebApplicationException(ex, SC_INTERNAL_SERVER_ERROR);
    } finally {
      deletePath(dataFilePath);
    }

  }

  private static void deletePath(java.nio.file.Path dataFilePath) {
    try {
        Files.delete(dataFilePath);
    } catch (Exception ex) {
      LOG.error("Error deleting file {}", dataFilePath, ex);
    }
  }

  /**
   * TODO move to validator-processor
   * Method responsible to extract metadata (and headers) from the file identified by dataFilePath.
   *
   * @param dataFilePath location of the file
   * @param dataFile this object will be updated directly
   */
  private static void extractAndSetTabularFileMetadata(java.nio.file.Path dataFilePath, DataFile dataFile) {
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
