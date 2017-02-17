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

  private final Optional<String> gangliaHost;
  private final Optional<Integer> gangliaPort;

  public static class Builder {
    private String apiUrl;
    private NormalizerConfiguration normalizerConfiguration;
    private URL extensionListURL;

    private Optional<String> gangliaHost;
    private Optional<Integer> gangliaPort;

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

    public Builder setGangliaHost(Optional<String> gangliaHost) {
      this.gangliaHost = gangliaHost;
      return this;
    }

    public Builder setGangliaPort(Optional<Integer> gangliaPort) {
      this.gangliaPort = gangliaPort;
      return this;
    }

    public ValidatorConfiguration build(){
      return new ValidatorConfiguration(apiUrl, normalizerConfiguration,
              extensionListURL, gangliaHost, gangliaPort);
    }
  }

  public static Builder builder(){
    return new Builder();
  }

  public ValidatorConfiguration(String apiUrl, NormalizerConfiguration normalizerConfiguration,
                                URL extensionListURL, Optional<String> gangliaHost, Optional<Integer> gangliaPort){
    this.apiUrl = apiUrl;
    this.normalizerConfiguration = normalizerConfiguration;
    this.extensionListURL = extensionListURL;

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

  public Optional<String> getGangliaHost() {
    return gangliaHost;
  }

  public Optional<Integer> getGangliaPort() {
    return gangliaPort;
  }
}
