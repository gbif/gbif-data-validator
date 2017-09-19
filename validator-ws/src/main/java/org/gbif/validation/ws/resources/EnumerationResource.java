package org.gbif.validation.ws.resources;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.ws.server.interceptor.NullToNotFound;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;

/**
 * A resource that provides a JSON serialization of validation related Enumerations.
 */
@Path("enumeration")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class EnumerationResource {
  private static final Map<String, Enum<?>[]> PATH_MAPPING = new HashMap<String, Enum<?>[]>();
  static {
    PATH_MAPPING.put(EvaluationCategory.class.getSimpleName(), EvaluationCategory.class.getEnumConstants());
    PATH_MAPPING.put(EvaluationType.class.getSimpleName(), EvaluationType.class.getEnumConstants());
    PATH_MAPPING.put(ValidationErrorCode.class.getSimpleName(), ValidationErrorCode.class.getEnumConstants());
    PATH_MAPPING.put(FileFormat.class.getSimpleName(), FileFormat.class.getEnumConstants());
  }

  /**
   * An inventory of the enumerations supported.
   *
   * @return The supported validation related enumerations.
   */
  @GET
  @Path("simple")
  public Set<String> inventory() {
    return PATH_MAPPING.keySet();
  }

  /**
   * Gets the values of the named enumeration should the enumeration exist.
   * The ordering of elements from the original enum is preserved.
   *
   * @param name name in the Java code as defined by AVAILABLE_ENUMS
   * @return The enumeration values or null if the enumeration does not exist.
   */
  @Path("simple/{name}")
  @GET
  @NullToNotFound
  public Enum<?>[] getEnumeration(@PathParam("name") @NotNull String name) {
    return PATH_MAPPING.getOrDefault(name, null);
  }
}
