package org.gbif.validation.dwc.extensions;

import org.gbif.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.ExtensionManager;
import org.gbif.dwc.extensions.ExtensionManagerFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.xml.SAXUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adapter of {ExtensionManagerFactory} to allow unit tests to get ExtensionManager run faster.
 *
 */
public class ExtensionManagerFactoryTestAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionManagerFactoryTestAdapter.class);

  public static final List<String> EXT_URL_TO_LOAD = Arrays.asList(
          "http://rs.gbif.org/extension/gbif/1.0/description.xml",
          "http://rs.gbif.org/core/dwc_occurrence_2015-07-02.xml");

  /**
   * Warning: this test {@link ExtensionManager} ignores vocabularies.
   * @param httpClient
   * @return
   */
  public static ExtensionManager getTestExtensionManager(HttpClient httpClient) {
    List<URL> testExtList =
            EXT_URL_TO_LOAD.stream()
                    .map(ext -> {
                      try {
                        return new URL(ext);
                      } catch (MalformedURLException e) {
                        LOG.error("Can NOT load extension definition", e);
                      }
                      return null;
                    })
                    .collect(Collectors.toList());
    return ExtensionManagerFactory.buildExtensionManager(buildTestExtensionFactory(), httpClient, testExtList,
            false);
  }

  /**
   * Create an empty {@link VocabulariesManager} to avoid loading all the vocabularies.
   *
   * @return
   */
  private static ExtensionFactory buildTestExtensionFactory() {
    VocabulariesManager vocabulariesManager = new VocabulariesManager(){
      @Override
      public Vocabulary get(String s) {
        return null;
      }

      @Override
      public Vocabulary get(URL url) {
        return null;
      }

      @Override
      public Map<String, String> getI18nVocab(String s, String s1) {
        return null;
      }

      @Override
      public List<Vocabulary> list() {
        return null;
      }
    };
    ThesaurusHandlingRule thesaurusRule = new ThesaurusHandlingRule(vocabulariesManager);

    return new ExtensionFactory(thesaurusRule,  SAXUtils.getNsAwareSaxParserFactory());
  }


}
