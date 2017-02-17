package org.gbif.validation.ws.conf;

import java.net.URL;
import java.util.Optional;

/**
 * Configuration settings required by the data validation web services.
 */
public class ValidationWsConfiguration {

  //name of the parameter used when POSTing a file
  public static final String FILE_POST_PARAM_NAME = "file";

  /**
   * Url to the GBIF Rest API.
   */
  private String apiUrl;

  private URL extensionDiscoveryUrl;

  private Optional<String> gangliaHost;
  private Optional<Integer> gangliaPort;

  /**
   * Maximum number of lines a file can contains until we split it.
   */
  private Integer fileSplitSize;

  /**
   * Directory used to copy data files to be validated.
   */
  private String workingDir;

  /**
   * Path to the data validation public path.
   */
  private String apiDataValidationPath;

  /**
   * Directory where FileJobStorage stores job results.
   */
  private String jobResultStorageDir;

  public String getApiUrl() {
    return apiUrl;
  }

  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  public String getApiDataValidationPath() {
    return apiDataValidationPath;
  }

  public void setExtensionDiscoveryUrl(URL extensionDiscoveryUrl){
    this.extensionDiscoveryUrl = extensionDiscoveryUrl;
  }

  public URL getExtensionDiscoveryUrl() {
    return extensionDiscoveryUrl;
  }

  public void setApiDataValidationPath(String apiDataValidationPath) {
    this.apiDataValidationPath = apiDataValidationPath;
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

  public String getJobResultStorageDir() {
    return jobResultStorageDir;
  }

  public void setJobResultStorageDir(String jobResultStorageDir) {
    this.jobResultStorageDir = jobResultStorageDir;
  }

  public Optional<String> getGangliaHost() {
    return gangliaHost;
  }

  public void setGangliaHost(Optional<String> gangliaHost) {
    this.gangliaHost = gangliaHost;
  }

  public Optional<Integer> getGangliaPort() {
    return gangliaPort;
  }

  public void setGangliaPort(Optional<Integer> gangliaPort) {
    this.gangliaPort = gangliaPort;
  }
}
