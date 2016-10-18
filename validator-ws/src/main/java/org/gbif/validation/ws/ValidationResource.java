package org.gbif.validation.ws;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.model.ValidationResult;
import org.gbif.occurrence.validation.tabular.OccurrenceDataFileProcessorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;
import org.gbif.occurrence.validation.api.model.DataFileDescriptor;
import org.gbif.utils.HttpUtil;
import org.gbif.ws.server.provider.DataFileDescriptorProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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

  private HttpUtil httpUtil;

  @Inject
  public ValidationResource(ValidationConfiguration configuration) {
    this.configuration = configuration;
    dataFileProcessorFactory = new OccurrenceDataFileProcessorFactory(configuration.getApiUrl());
    httpUtil = new HttpUtil(HttpUtil.newMultithreadedClient(60000,10,2));
  }



  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/file")
  public ValidationResult validateFile(@FormDataParam(FILE_PARAM) final InputStream stream,
                                       @FormDataParam(FILE_PARAM) FormDataContentDisposition header,
                                       FormDataMultiPart formDataMultiPart) {
    try {
      DataFileDescriptor dataFileDescriptor = DataFileDescriptorProvider.getValue(formDataMultiPart, header);
      java.nio.file.Path dataFilePath = copyFile(dataFileDescriptor, stream);
      return processFile(dataFilePath, dataFileDescriptor);
    } catch (Exception ex) {
      throw  new WebApplicationException(Response.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private  java.nio.file.Path copyFile(DataFileDescriptor dataFileDescriptor, final InputStream stream) {
    if(dataFileDescriptor.getFile() != null) {
      String lwrValue = dataFileDescriptor.getFile().toLowerCase();
      try {
        if (lwrValue.startsWith("http://") || lwrValue.startsWith("https://")) {
          URL fileUrl = new URL(lwrValue);
          java.nio.file.Path destinyFilePath = Paths.get(configuration.getWorkingDir(), UUID.randomUUID().toString(),
                                                         Paths.get(lwrValue).getFileName().toString());
          httpUtil.download(fileUrl,destinyFilePath.toFile());
          return destinyFilePath;
        } else {
          java.nio.file.Path destinyFilePath = Paths.get(configuration.getWorkingDir(), UUID.randomUUID().toString(),
                                                         dataFileDescriptor.getFile());
          copyDataFile(stream,destinyFilePath);
          return destinyFilePath;
        }
      } catch(IOException  ex){
        throw new WebApplicationException(ex, javax.ws.rs.core.Response.Status.BAD_REQUEST);
      }
    }
    throw new WebApplicationException(javax.ws.rs.core.Response.Status.BAD_REQUEST);
  }


  /**
   * Copies the input stream into a temporary directory.
   */
  private void copyDataFile(final InputStream stream,
                                          java.nio.file.Path destinyDataFilePath) throws IOException {
    LOG.info("Uploading data file into {}", destinyDataFilePath.toString());
    Files.createDirectory(destinyDataFilePath.getParent());
    //Delete on exit JVM
    destinyDataFilePath.toFile().deleteOnExit();
    Files.copy(stream, destinyDataFilePath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Applies the validation routines to the input file.
   */
  private ValidationResult processFile(java.nio.file.Path dataFilePath, DataFileDescriptor dataFileDescriptor) throws IOException {
    DataFile dataFile = new DataFile();
    dataFile.setFileName(dataFilePath.toFile().getAbsolutePath());
    dataFile.setNumOfLines(FileBashUtilities.countLines(dataFilePath.toFile().getAbsolutePath()));
    dataFile.setDelimiterChar(dataFileDescriptor.getFieldsTerminatedBy());
    dataFile.setHasHeaders(dataFileDescriptor.isHasHeaders());
    dataFile.loadHeaders();
    return dataFileProcessorFactory.create(dataFile).process(dataFile);
  }
}
