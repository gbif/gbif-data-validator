package org.gbif.validation.ws;


public class ValidationConfiguration {

  /**
   * Url to the GBIF Rest API.
   */
  private String apiUrl;

  /**
   * Directory used to copy data files to be validated.
   */
  private String workingDir;

  public String getApiUrl() {
    return apiUrl;
  }

  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  public String getWorkingDir() {
    return workingDir;
  }

  public void setWorkingDir(String workingDir) {
    this.workingDir = workingDir;
  }
}
