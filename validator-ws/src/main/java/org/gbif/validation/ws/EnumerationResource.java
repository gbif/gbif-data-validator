package org.gbif.validation.ws;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationErrorCode;
import org.gbif.ws.server.interceptor.NullToNotFound;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resource that provides a JSON serialization of validation related Enumerations.
 */
@Path("enumeration")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class EnumerationResource {

  private static final Logger LOG = LoggerFactory.getLogger(EnumerationResource.class);

  private static final List<Class<? extends Enum>> AVAILABLE_ENUMS =
          Arrays.asList(
                  EvaluationCategory.class,
                  EvaluationType.class,
                  ValidationErrorCode.class,
                  FileFormat.class);

  private static Map<String, Enum<?>[]> PATH_MAPPING = initEnumerations();

  /**
   * Initialize enumeration elements based on Enumeration defined in AVAILABLE_ENUMS.
   *
   * @return
   */
  private static Map<String, Enum<?>[]> initEnumerations() {
    Map<String, Enum<?>[]> enumMap = new HashMap<>();
    try {
      for (Class<? extends Enum> enumClass : AVAILABLE_ENUMS) {
        // verify that it is an Enumeration
        if (enumClass.getEnumConstants() != null) {
          //enum getEnumConstants preserves the order in which the enum constants are defined
          enumMap.put(enumClass.getSimpleName(), enumClass.getEnumConstants());
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to read enumeration value(s)", e);
      //do no return a partial map
      enumMap.clear();
    }
    return Collections.unmodifiableMap(enumMap);
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
    if (PATH_MAPPING.containsKey(name)) {
      return PATH_MAPPING.get(name);
    } else {
      return null;
    }
  }
}
