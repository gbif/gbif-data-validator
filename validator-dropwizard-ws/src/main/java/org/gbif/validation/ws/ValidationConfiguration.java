package org.gbif.validation.ws;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

public class ValidationConfiguration extends Configuration {

  /**
   * Url to the GBIF Rest API.
   */
  @NotEmpty
  private String apiUrl;

  /**
   * Directory used to copy data files to be validated.
   */
  @NotEmpty
  private String workingDir;

  @JsonProperty
  public String getApiUrl() {
    return apiUrl;
  }

  @JsonProperty
  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  @JsonProperty
  public String getWorkingDir() {
    return workingDir;
  }

  @JsonProperty
  public void setWorkingDir(String workingDir) {
    this.workingDir = workingDir;
  }
}
