package org.gbif.validation.ws;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.tabular.OccurrenceDataFileProcessorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;
import org.gbif.occurrence.validation.api.DataFileDescriptor;
import org.gbif.ws.server.provider.DataFileDescriptorProvider;

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
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.FormDataParam;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
public class ValidationResource {

  private final ValidationConfiguration configuration;

  private final OccurrenceDataFileProcessorFactory dataFileProcessorFactory;

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private static final String FILE_PARAM = "file";

  @Inject
  public ValidationResource(ValidationConfiguration configuration) {
    this.configuration = configuration;
    dataFileProcessorFactory = new OccurrenceDataFileProcessorFactory(configuration.getApiUrl());
  }



  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public String validateFile(@FormDataParam(FILE_PARAM) final InputStream stream,
                             @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                             FormDataMultiPart formDataMultiPart) {
    try {
      DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart);
      dataFileDescriptor.setFileName(header.getFileName());
      java.nio.file.Path dataFilePath = copyDataFile(stream, header);
      return processFile(dataFilePath, dataFileDescriptor).toString();
    } catch (IOException  ex) {
      throw  new WebApplicationException(Response.SC_INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Copies the input stream into a temporary directory.
   */
  private java.nio.file.Path copyDataFile(final InputStream stream, final FormDataContentDisposition header) throws IOException {
    java.nio.file.Path dataFilePath = Paths.get(configuration.getWorkingDir(), UUID.randomUUID().toString(),
                                                header.getFileName());
    LOG.info("Uploading data file {} into {}", header.getFileName(), dataFilePath.toString());
    Files.createDirectory(dataFilePath.getParent());
    //Delete on exit JVM
    dataFilePath.toFile().deleteOnExit();
    Files.copy(stream, dataFilePath, StandardCopyOption.REPLACE_EXISTING);
    return dataFilePath;
  }

  /**
   * Applies the validation routines to the input file.
   */
  private DataFileValidationResult processFile(java.nio.file.Path dataFilePath, DataFileDescriptor dataFileDescriptor) throws IOException {
    DataFile dataFile = new DataFile();
    dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
    dataFile.setNumOfLines(FileBashUtilities.countLines(dataFilePath.toFile().getAbsolutePath()));
    dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
    dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
    dataFile.loadHeaders();
    return dataFileProcessorFactory.create(dataFile.getNumOfLines()).process(dataFile);
  }
}
