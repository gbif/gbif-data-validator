package org.gbif.validation.ws.provider;

import org.gbif.validation.ws.resources.FileSizeException;
import org.gbif.validation.ws.resources.UploadedFileManager;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.google.inject.Singleton;


/**
 * ExceptionMapper mapper to return 413 - Payload Too Large on {@link UploadedFileManager.FileDownloadSizeException}
 */
@Provider
@Singleton
public class FileSizeExceptionMapper implements ExceptionMapper<FileSizeException> {
  @Override
  public Response toResponse(FileSizeException e) {
    return Response.status(413).build();
  }
}
