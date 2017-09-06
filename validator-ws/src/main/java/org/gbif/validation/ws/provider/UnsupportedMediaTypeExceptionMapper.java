package org.gbif.validation.ws.provider;

import org.gbif.exception.UnsupportedMediaTypeException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.google.inject.Singleton;

/**
 * ExceptionMapper mapper to return UNSUPPORTED_MEDIA_TYPE on {@link UnsupportedMediaTypeException}
 */
@Provider
@Singleton
public class UnsupportedMediaTypeExceptionMapper implements ExceptionMapper<UnsupportedMediaTypeException> {
  @Override
  public Response toResponse(UnsupportedMediaTypeException e) {
    return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE).entity(e.getMessage()).build();
  }
}



