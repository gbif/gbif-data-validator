package org.gbif.validation.ws.utils;

import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationResultBuilders;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Utility class to build error responses.
 */
public class WebErrorUtils {

  /**
   * Private constructor.
   */
  private WebErrorUtils() {
    //empty
  }

  /**
   * Prepare a {@link Response} object.
   *
   * @param uploadFileName
   * @param status
   * @param errorCode
   * @return
   */
  public static WebApplicationException errorResponse(String uploadFileName, Response.Status status,
                                                      ValidationErrorCode errorCode) {
    Response.ResponseBuilder repBuilder = Response.status(status);
    repBuilder.entity(ValidationResultBuilders.Builder.withError(uploadFileName, null, errorCode).build());
    return new WebApplicationException(repBuilder.build());
  }

  public static WebApplicationException errorResponse(Response.Status status,
                                                      ValidationErrorCode errorCode) {
    return errorResponse(null,status,errorCode);
  }
}
