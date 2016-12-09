package org.gbif.validation.ws.resources;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.jobserver.JobServer;
import org.gbif.validation.ws.conf.ValidationConfiguration;

import java.io.IOException;
import java.net.URI;
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
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.ClientResponse;

/**
 *  Asynchronous web resource to process data validations.
 *  Internally, redirects all the requests to a JobServer instances that coordinates all data validations.
 */
@Path("/jobserver")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ValidationResource {

  private static final String STATUS_PATH = "/status/";

  private final UploadedFileManager fileTransferManager;
  private final JobServer<?> jobServer;
  private final ValidationConfiguration configuration;


  private URI getJobRedirectUri(long jobId) {
    return URI.create(configuration.getApiDataValidationPath() + STATUS_PATH + jobId);
  }

  /**
   * Builds a Jersey response from a JobStatusResponse instance.
   */
  private Response buildResponseFromStatus(JobStatusResponse<?> jobResponse) {
    if (JobStatusResponse.JobStatus.ACCEPTED == jobResponse.getStatus()) {
      return Response.seeOther(getJobRedirectUri(jobResponse.getJobId())).status(Response.Status.ACCEPTED)
                      .entity(jobResponse).build();
    } else if (JobStatusResponse.JobStatus.NOT_FOUND == jobResponse.getStatus()) {
      return Response.status(ClientResponse.Status.NOT_FOUND).entity(jobResponse).build();
    } else {
      return Response.ok(jobResponse).build();
    }
  }

  @Inject
  public ValidationResource(ValidationConfiguration configuration, JobServer<ValidationResult> jobServer)
    throws IOException {
    this.jobServer = jobServer;
    this.configuration = configuration;
    fileTransferManager = new UploadedFileManager(configuration.getWorkingDir());
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/submit")
  public Response submit(@Context HttpServletRequest request) {
    Optional<DataFile> dataFile = fileTransferManager.uploadDataFile(request);
    if (dataFile.isPresent()) {
      return buildResponseFromStatus(jobServer.submit(dataFile.get()));
    }
    return Response.status(Response.Status.BAD_REQUEST).entity(JobStatusResponse.FAILED_RESPONSE).build();
  }


  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path(STATUS_PATH + "{jobid}")
  public Response status(@PathParam("jobid") String jobid) {
    return buildResponseFromStatus(jobServer.status(Long.valueOf(jobid)));
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/status/{jobid}/kill")
  public Response kill(@PathParam("jobid") String jobid) {
    return buildResponseFromStatus(jobServer.kill(Long.valueOf(jobid)));
  }

}
