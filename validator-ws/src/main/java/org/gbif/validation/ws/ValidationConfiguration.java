package org.gbif.validation.ws;

/**
 * Configuration settings required by the data validation web services.
 */
public class ValidationConfiguration {

  /**
   * Url to the GBIF Rest API.
   */
  private String apiUrl;

  /**
   * Maximum number of lines a file can contains until we split it.
   */
  private Integer fileSplitSize;

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

  public Integer getFileSplitSize() {
    return fileSplitSize;
  }

  public void setFileSplitSize(Integer fileSplitSize) {
    this.fileSplitSize = fileSplitSize;
  }

  public String getWorkingDir() {
    return workingDir;
  }

  public void setWorkingDir(String workingDir) {
    this.workingDir = workingDir;
  }
}
