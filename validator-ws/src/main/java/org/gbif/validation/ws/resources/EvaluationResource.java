package org.gbif.validation.ws.resources;

import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.evaluator.IndexableRules;

import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;

/**
 *
 */
@Path("evaluation")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class EvaluationResource {

  /**
   * An inventory of the enumerations supported.
   *
   * @return The supported validation related enumerations.
   */
  @GET
  @Path("nonindexable")
  public Set<EvaluationType> inventory() {
    return IndexableRules.getNonIndexableEvaluationType();
  }
}
