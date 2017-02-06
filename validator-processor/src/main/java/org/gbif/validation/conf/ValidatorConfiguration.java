package org.gbif.validation.conf;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;

import java.net.URL;

/**
 * Configuration holder for the validator.
 */
public class ValidatorConfiguration {

  private final String apiUrl;
  private final NormalizerConfiguration normalizerConfiguration;
  private final URL extensionListURL;

  public ValidatorConfiguration(String apiUrl, NormalizerConfiguration normalizerConfiguration,
                                URL extensionListURL){
    this.apiUrl = apiUrl;
    this.normalizerConfiguration = normalizerConfiguration;
    this.extensionListURL = extensionListURL;
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
}
