package org.gbif.dwc.extensions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.collect.Maps;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for all vocabulary related methods. Keeps an internal map of locally existing and parsed vocabularies which
 * is keyed on a normed filename derived from a vocabularies URL. We use this derived filename instead of the proper URL
 * as we do not persist any additional data than the extension file itself - which doesnt have its own URL embedded.
 *
 * Moved from https://github.com/gbif/dwca-validator3
 */
public class CachedVocabulariesManager implements VocabulariesManager {

  private static final Logger LOG = LoggerFactory.getLogger(CachedVocabulariesManager.class);
  private final VocabularyFactory vocabFactory;
  private final HttpClient httpClient;

  private Map<String, Vocabulary> vocabularies = Maps.newHashMap();
  private Map<String, String> uri2url = Maps.newHashMap();


  public CachedVocabulariesManager(VocabularyFactory vocabFactory, HttpClient httpClient) {
    this.vocabFactory = vocabFactory;
    this.httpClient = httpClient;
  }

  private boolean addToCache(Vocabulary v, String url) {
    if (url == null) {
      LOG.error("Cannot add vocabulary {} to cache without a valid URL", v.getUri());
      return false;
    }
    uri2url.put(v.getUri().toLowerCase(), url);
    // keep vocab in local lookup
    if (vocabularies.containsKey(url)) {
      LOG.warn("Vocabulary URI {} exists already - overwriting with new vocabulary from {}",v.getUri(), url);
    }
    vocabularies.put(url, v);
    return true;
  }

  @Override
  public Vocabulary get(String uri) {
    if (uri == null) {
      return null;
    }
    String url = uri2url.get(uri.toLowerCase());
    return vocabularies.get(url);
  }

  @Override
  public Vocabulary get(URL url) {
    String urlString = url.toString();
    if (!vocabularies.containsKey(urlString)) {
      install(urlString);
    }
    return vocabularies.get(urlString);
  }

  @Override
  public Map<String, String> getI18nVocab(String uri, String lang) {
    Map<String, String> map = new HashMap<>();
    Vocabulary v = get(uri);
    if (v != null) {
      for (VocabularyConcept c : v.getConcepts()) {
        VocabularyTerm t = c.getPreferredTerm(lang);
        map.put(c.getIdentifier(), t == null ? c.getIdentifier() : t.getTitle());
      }
    }
    if (map.isEmpty()) {
      LOG.debug("Empty i18n map for vocabulary " + uri + " and language " + lang);
    }
    return map;
  }

  /**
   * Downloads vocabulary into local file for subsequent IPT startups
   * and adds the vocab to the internal cache.
   * Downloads use a conditional GET, i.e. only download the vocabulary files if the content has been changed since the
   * last download.
   * lastModified dates are taken from the filesystem.
   *
   * @param url
   * @return
   * @throws IOException
   */
  private void install(String url) {
    if (url != null) {
      // parse vocabulary file
      HttpGet get = new HttpGet(url);
      HttpEntity entity = null;
      try {
        HttpResponse response = httpClient.execute(get);
        entity = response.getEntity();
        if (entity != null) {
          InputStream is = entity.getContent();
          Vocabulary v = vocabFactory.build(is);
          EntityUtils.consume(entity);
          v.setLastUpdate(new Date());
          LOG.info("Successfully loaded Vocabulary: " + v.getTitle());
          addToCache(v, url);
        }
      } catch (ParserConfigurationException e) {
        LOG.error("ParserConfigurationException: {}", e.getMessage());
      } catch (Exception e) {
        LOG.error("Failed to install vocabulary {} : {}", url, e.getMessage());
      } finally {
        if (entity != null) {
          try {
            EntityUtils.consume(entity);
          } catch (IOException e) {
            LOG.warn("Cannot consume vocabulary http entity", e);
          }
        }
      }
    } else {
      LOG.warn("Ignore null vocabulary {}", url);
    }
  }

  @Override
  public List<Vocabulary> list() {
    return new ArrayList<>(vocabularies.values());
  }

}
