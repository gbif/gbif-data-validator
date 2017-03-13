package org.gbif.validation.ws.utils;

import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.result.ValidationResult;

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
   * Builds a WebApplicationException using the supplied parameters.
   */
  public static WebApplicationException errorResponse(String uploadFileName, Response.Status status,
                                                      ValidationErrorCode errorCode) {
    Response.ResponseBuilder repBuilder = Response.status(status);
    repBuilder.entity(ValidationResult.onError(uploadFileName, null, errorCode, null));
    return new WebApplicationException(repBuilder.build());
  }

  /**
   * Builds a WebApplicationException using the supplied parameters.
   */
  public static WebApplicationException errorResponse(Response.Status status,
                                                      ValidationErrorCode errorCode) {
    return errorResponse(null, status, errorCode);
  }
}
