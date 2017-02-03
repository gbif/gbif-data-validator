package org.gbif.dwc.extensions;

import org.gbif.digester.ThesaurusHandlingRule;
import org.gbif.xml.SAXUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory responsible for building {@link ExtensionManager} instance.
 */
public class ExtensionManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionManagerFactory.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Builds and wires a new ExtensionManager by retrieving the extensions from a discovery URL.
   *
   * @param httpClient
   * @param extensionDiscoveryURL
   * @param autoUpdate should the {@link ExtensionManager} updates itself at a regular interval
   *
   * @return
   */
  public static ExtensionManager buildExtensionManager(HttpClient httpClient, URL extensionDiscoveryURL,
                                                       boolean autoUpdate) {
    Objects.requireNonNull(httpClient, "httpClient shall be provided");
    Objects.requireNonNull(extensionDiscoveryURL, "extensionDiscoveryURL shall be provided");

    return new ExtensionManager(buildExtensionFactory(httpClient), httpClient,
            discoverExtensions(extensionDiscoveryURL), autoUpdate);
  }

  /**
   * Builds and wires a new ExtensionManager using the list of extension URL.
   *
   * @param httpClient
   * @param extensionListURL
   * @param autoUpdate should the {@link ExtensionManager} updates itself at a regular interval
   *
   * @return
   */
  public static ExtensionManager buildExtensionManager(HttpClient httpClient, List<URL> extensionListURL,
                                                       boolean autoUpdate) {
    Objects.requireNonNull(httpClient, "httpClient shall be provided");
    Objects.requireNonNull(extensionListURL, "extensionListURL shall be provided");
    return new ExtensionManager(buildExtensionFactory(httpClient), httpClient, extensionListURL, autoUpdate);
  }

  /**
   * Inner helper function to wire the {@link ExtensionFactory}.
   * @param httpClient
   * @return
   */
  private static ExtensionFactory buildExtensionFactory(HttpClient httpClient) {
    VocabulariesManager vocabulariesManager = new CachedVocabulariesManager(
            new VocabularyFactory(SAXUtils.getNsAwareSaxParserFactory()), httpClient);
    ThesaurusHandlingRule thesaurusRule = new ThesaurusHandlingRule(vocabulariesManager);

    return new ExtensionFactory(thesaurusRule,  SAXUtils.getNsAwareSaxParserFactory());
  }

  /**
   * Retrieve a list of Extensions URL from an endpoint
   * @param extensionDiscoveryURL
   * @return
   */
  private static List<URL> discoverExtensions(URL extensionDiscoveryURL) {
    List<URL> extensions = new ArrayList<>();
    try {
      // get json
      LOG.info("Retrieving extensions from " + extensionDiscoveryURL);
      Map<String, Object> registryResponse = MAPPER.readValue(extensionDiscoveryURL, Map.class);
      List<Map<String, Object>> jsonExtensions = (List<Map<String, Object>>) registryResponse.get("extensions");
      for (Map<String, Object> ext : jsonExtensions) {
        try {
          extensions.add(new URL((String) ext.get("url")));
        } catch (Exception e) {
          LOG.error("Exception when listing extensions", e);
        }
      }
      LOG.info("Discovered {} extensions.", extensions.size());
    } catch (MalformedURLException e) {
      LOG.error("MalformedURLException when discovering extensions", e);
    } catch (IOException e) {
      LOG.error("IOException when discovering extensions", e);
    }
    return extensions;
  }
}
