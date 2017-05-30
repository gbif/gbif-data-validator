package org.gbif.validation.conf;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;

import java.net.URL;
import java.util.Optional;

/**
 * Configuration holder for the validator.
 * This class is immutable.
 */
public class ValidatorConfiguration {

  private final String apiUrl;
  private final NormalizerConfiguration normalizerConfiguration;
  private final URL extensionListURL;

  private final boolean preserveTemporaryFiles;

  private final String gangliaHost;
  private final Integer gangliaPort;

  //eventually we could allow to not run it for very large datasets
  private final boolean runOccurrenceInterpretation = true;

  public static class Builder {
    private String apiUrl;
    private NormalizerConfiguration normalizerConfiguration;
    private URL extensionListURL;
    private boolean preserveTemporaryFiles = false;

    private String gangliaHost;
    private Integer gangliaPort;

    public Builder setApiUrl(String apiUrl) {
      this.apiUrl = apiUrl;
      return this;
    }

    public Builder setNormalizerConfiguration(NormalizerConfiguration normalizerConfiguration) {
      this.normalizerConfiguration = normalizerConfiguration;
      return this;
    }

    public Builder setExtensionListURL(URL extensionListURL) {
      this.extensionListURL = extensionListURL;
      return this;
    }

    public Builder setPreserveTemporaryFiles(boolean preserveTemporaryFiles) {
      this.preserveTemporaryFiles = preserveTemporaryFiles;
      return this;
    }

    public Builder setGangliaHost(String gangliaHost) {
      this.gangliaHost = gangliaHost;
      return this;
    }

    public Builder setGangliaPort(Integer gangliaPort) {
      this.gangliaPort = gangliaPort;
      return this;
    }

    public ValidatorConfiguration build(){
      return new ValidatorConfiguration(apiUrl, normalizerConfiguration,
              extensionListURL, preserveTemporaryFiles , gangliaHost, gangliaPort);
    }
  }

  public static Builder builder(){
    return new Builder();
  }

  public ValidatorConfiguration(String apiUrl, NormalizerConfiguration normalizerConfiguration,
                                URL extensionListURL,  boolean preserveTemporaryFiles,
                                String gangliaHost, Integer gangliaPort){
    this.apiUrl = apiUrl;
    this.normalizerConfiguration = normalizerConfiguration;
    this.extensionListURL = extensionListURL;
    this.preserveTemporaryFiles = preserveTemporaryFiles;

    this.gangliaHost = gangliaHost;
    this.gangliaPort = gangliaPort;
  }

  public String getApiUrl() {
    return apiUrl;
  }

  public NormalizerConfiguration getNormalizerConfiguration() {
    return normalizerConfiguration;
  }

  public URL getExtensionListURL() {
    return extensionListURL;
  }

  /**
   * Should the temporary created for validation should be preserved.
   * This is mostly for debugging purpose.
   * @return
   */
  public boolean isPreservedTemporaryFiles() {
    return preserveTemporaryFiles;
  }


  public boolean isRunOccurrenceInterpretation() {
    return runOccurrenceInterpretation;
  }

  public Optional<String> getGangliaHost() {
    return Optional.ofNullable(gangliaHost);
  }

  public Optional<Integer> getGangliaPort() {
    return Optional.ofNullable(gangliaPort);
  }
}
