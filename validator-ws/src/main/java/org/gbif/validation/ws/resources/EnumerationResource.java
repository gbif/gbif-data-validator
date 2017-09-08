package org.gbif.validation.ws.resources;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.ws.server.interceptor.NullToNotFound;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

  private static final List<Class<? extends Enum<?>>> AVAILABLE_ENUMS = Collections.unmodifiableList(
    Arrays.asList(EvaluationCategory.class, EvaluationType.class, ValidationErrorCode.class, FileFormat.class));

  private static final Map<String, Enum<?>[]> PATH_MAPPING = initEnumerations();

  /**
   * Initialize enumeration elements based on Enumeration defined in AVAILABLE_ENUMS.
   *
   * @return
   */
  private static Map<String, Enum<?>[]> initEnumerations() {
    return AVAILABLE_ENUMS.stream().collect(Collectors.toMap(Class::getSimpleName, Class::getEnumConstants));
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
