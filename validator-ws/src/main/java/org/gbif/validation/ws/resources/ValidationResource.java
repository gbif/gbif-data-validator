package org.gbif.validation.ws.resources;

import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.ws.conf.ValidationConfiguration;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.server.Response.SC_INTERNAL_SERVER_ERROR;
import static org.gbif.validation.ws.utils.WebErrorUtils.errorResponse;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationResource {

  private final ResourceEvaluationManager resourceEvaluationManager;

  private final UploadedFileManager fileTransferManager;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  @Inject
  public ValidationResource(ValidationConfiguration configuration, ResourceEvaluationManager resourceEvaluationManager)
    throws IOException {
    this.resourceEvaluationManager = resourceEvaluationManager;
    fileTransferManager = new UploadedFileManager(configuration.getWorkingDir());
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public ValidationResult onValidateFile(@Context HttpServletRequest request) {
    Optional<DataFile> dataFile = fileTransferManager.uploadDataFile(request);
    return processFile(dataFile);
  }


  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/url")
  public ValidationResult onValidateFile(@QueryParam("fileUrl") String fileURL) {
    try {
      Optional<DataFile> dataFile = fileTransferManager.uploadDataFile(new URL(fileURL));
      return processFile(dataFile);
    } catch (IOException ioEx) {
      LOG.error("Can't handle file download from {}", fileURL, ioEx);
      throw errorResponse(fileURL, Response.Status.BAD_REQUEST, ValidationErrorCode.IO_ERROR);
    }
  }


  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(Optional<DataFile> dataFile)  {
    if (dataFile.isPresent()) {
      try {
        return resourceEvaluationManager.evaluate(dataFile.get());
      } catch (Exception ex) {
        throw new WebApplicationException(ex, SC_INTERNAL_SERVER_ERROR);
      } finally {
        deletePath(dataFile.get().getFilePath());
      }
    }
    throw errorResponse(Response.Status.BAD_REQUEST, ValidationErrorCode.UNSUPPORTED_FILE_FORMAT);
  }

  private static void deletePath(java.nio.file.Path dataFilePath) {
    try {
        Files.delete(dataFilePath);
    } catch (Exception ex) {
      LOG.error("Error deleting file {}", dataFilePath, ex);
    }
  }


}
