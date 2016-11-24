package org.gbif.validation.ws.resources;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.jobserver.JobServer;
import org.gbif.validation.ws.conf.ValidationConfiguration;

import java.io.IOException;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Path("/jobserver")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationJobServerResource {

  private final UploadedFileManager fileTransferManager;
  private final JobServer<?> jobServer;

  @Inject
  public ValidationJobServerResource(ValidationConfiguration configuration, JobServer<ValidationResult> jobServer)
    throws IOException {
    this.jobServer = jobServer;
    fileTransferManager = new UploadedFileManager(configuration.getWorkingDir());
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/submit")
  public JobStatusResponse onValidateFileAsync(@Context HttpServletRequest request) {
    Optional<DataFile> dataFile = fileTransferManager.uploadDataFile(request);
    if (dataFile.isPresent()) {
       return jobServer.submit(dataFile.get());
    }
    return JobStatusResponse.FAILED_RESPONSE;
  }


  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/status/{jobid}")
  public JobStatusResponse status(@PathParam("jobid") String jobid) {
    return jobServer.status(Long.valueOf(jobid));
  }

}
