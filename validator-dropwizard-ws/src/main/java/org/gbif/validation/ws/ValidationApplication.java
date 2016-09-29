package org.gbif.validation.ws;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

public class ValidationApplication extends Application<ValidationConfiguration> {

  @Override
  public String getName() {
    return "gbif-data-validator";
  }

  @Override
  public void initialize(Bootstrap<ValidationConfiguration> bootstrap) {
    // nothing to do yet
  }

  @Override
  public void run(ValidationConfiguration configuration, Environment environment) {
    ValidationResource validationResource = new ValidationResource(configuration);
    environment.jersey().register(validationResource);
    environment.jersey().register(MultiPartFeature.class);
  }
}
