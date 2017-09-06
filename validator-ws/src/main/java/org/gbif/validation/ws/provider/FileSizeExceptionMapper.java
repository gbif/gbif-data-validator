package org.gbif.validation.ws.provider;

import org.gbif.validation.ws.file.FileSizeException;
import org.gbif.ws.response.GbifResponseStatus;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.google.inject.Singleton;


/**
 * ExceptionMapper mapper to return 413 - Payload Too Large on {@link FileSizeException}
 */
@Provider
@Singleton
public class FileSizeExceptionMapper implements ExceptionMapper<FileSizeException> {
  @Override
  public Response toResponse(FileSizeException e) {
    return Response.status(GbifResponseStatus.PAYLOAD_TOO_LARGE.getStatus()).build();
  }
}
