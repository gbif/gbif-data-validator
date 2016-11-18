package org.gbif.validation.ws.resources;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;

@Path("/validate")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class AsyncValidationResource {


}
