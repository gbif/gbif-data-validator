package org.gbif.validation.ws;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.tabular.OccurrenceDataFileProcessorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;

import java.io.IOException;
import java.io.InputStream;
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
import com.sun.jersey.multipart.FormDataParam;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
public class ValidationResource {

  private final ValidationConfiguration configuration;

  private final OccurrenceDataFileProcessorFactory dataFileProcessorFactory;

  static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  @Inject
  public ValidationResource(ValidationConfiguration configuration) {
    this.configuration = configuration;
    dataFileProcessorFactory = new OccurrenceDataFileProcessorFactory(configuration.getApiUrl());
  }



  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public String validateFile(@FormDataParam("file") final InputStream stream,
                             @FormDataParam("file") FormDataContentDisposition header) {
    try {
      java.nio.file.Path dataFilePath = Paths.get(configuration.getWorkingDir(), UUID.randomUUID().toString(),
                                              header.getFileName());
      LOG.info("Uploading data file {} into {}", header.getFileName(), dataFilePath.toString());
      Files.createDirectory(dataFilePath.getParent());
      Files.copy(stream, dataFilePath, StandardCopyOption.REPLACE_EXISTING);
      return processFile(dataFilePath).toString();
    } catch (IOException  ex) {
      throw  new WebApplicationException(Response.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private DataFileValidationResult processFile(java.nio.file.Path dataFilePath) throws IOException {
    DataFile dataFile = new DataFile();
    dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
    dataFile.setNumOfLines(FileBashUtilities.countLines(dataFilePath.toFile().getAbsolutePath()));
    dataFile.setDelimiterChar('\t');
    dataFile.setHasHeaders(true);
    dataFile.loadHeaders();
    return dataFileProcessorFactory.create(dataFile.getNumOfLines()).process(dataFile);
  }
}
