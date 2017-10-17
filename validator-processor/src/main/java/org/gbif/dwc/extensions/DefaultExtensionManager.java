package org.gbif.dwc.extensions;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default manager for {@link Extension}.
 *
 * Moved from https://github.com/gbif/dwca-validator3
 */
class DefaultExtensionManager implements ExtensionManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultExtensionManager.class);

  class UpdateExtensionsTask extends TimerTask {
    @Override
    public void run() {
      LOG.info("Start updating extensions from registry");
      updateFromRegistry();
    }
  }

  private static final TermFactory TF = TermFactory.instance();
  private static final int AUTO_UPDATE_FREQUENCY_MS = 60 * 60 * 1000; //1 hour

  private final Supplier<List<URL>> extensionsUrlsSupplier;
  private final ExtensionFactory factory;
  private final HttpClient httpClient;

  private Map<Term, Extension> extensionsByRowtype = new ConcurrentHashMap<>();

  private final String TAXON_KEYWORD = "dwc:taxon";
  private final String OCCURRENCE_KEYWORD = "dwc:occurrence";
  private final Term OCCURRENCE_DWC = DwcTerm.Occurrence;
  private final Term SIMPLE_DWC = TF.findTerm("http://rs.tdwg.org/dwc/xsd/simpledarwincore/SimpleDarwinRecord");
  private Date registryUpdate;

  private final Timer timer = new Timer();

  /**
   * {@link DefaultExtensionManager} main constructor.
   * @param factory
   * @param httpClient
   * @param extensionUrlSupplier {@Link Supplier} to get a fresh copy of the extension URLs to load
   * @param autoUpdate
   */
  public DefaultExtensionManager(ExtensionFactory factory, HttpClient httpClient, Supplier<List<URL>> extensionUrlSupplier,
                                 boolean autoUpdate) {
    this.factory = factory;
    this.httpClient = httpClient;
    this.extensionsUrlsSupplier = extensionUrlSupplier;

    if(autoUpdate) {
      // scheduled for every hour
      this.timer.scheduleAtFixedRate(new DefaultExtensionManager.UpdateExtensionsTask(), new Date(), AUTO_UPDATE_FREQUENCY_MS);
    }else{
      //run once now
      updateFromRegistry();
    }
  }

  @Override
  public Extension get(Term rowType) {
    if (SIMPLE_DWC.equals(rowType)) {
      rowType = OCCURRENCE_DWC;
    }
    return extensionsByRowtype.get(rowType);
  }

  public Date getRegistryUpdate() {
    return registryUpdate;
  }

  /**
   * Install an {@link Extension} in memory from a URL.
   * @param url
   */
  private void install(URL url) {
    HttpGet get = new HttpGet(url.toString());
    HttpEntity entity = null;
    try {
      HttpResponse response = httpClient.execute(get);
      entity = response.getEntity();
      if (entity != null) {
        InputStream is = entity.getContent();
        //FIXME the flag "false", the concept of dev (sandbox) should be moved to the ExtensionFactory
        Extension ext = factory.build(is, url, false);
        if (ext != null && ext.getRowType() == null) {
          LOG.error("Extension {} lacking required rowType!", url);
        } else {
          // keep vocab in memory
          extensionsByRowtype.put(ext.getRowType(), ext);
          LOG.info("Successfully loaded extension {}", ext.getRowType());
        }
      }
    } catch (Exception e) {
      LOG.error("Error loading extension {}", url, e);
    } finally {
      if (entity != null) {
        try {
          EntityUtils.consume(entity);
        } catch (IOException e) {
          LOG.warn("Cannot consume extension http entity", e);
        }
      }
    }
  }

  @Override
  public List<Extension> list() {
    return new ArrayList<>(extensionsByRowtype.values());
  }

  public List<Extension> list(Extension core) {
    if (core != null && core.getRowType() == DwcTerm.Occurrence) {
      return search(OCCURRENCE_KEYWORD);
    } else if (core != null && core.getRowType() == DwcTerm.Taxon) {
      return search(TAXON_KEYWORD);
    } else {
      return list();
    }
  }

  public List<Extension> listCore() {
    List<Extension> list = new ArrayList<>();
    Extension e = get(DwcTerm.Occurrence);
    if (e != null) {
      list.add(e);
    }
    e = get(DwcTerm.Taxon);
    if (e != null) {
      list.add(e);
    }
    return list;
  }

  @Override
  public Map<Term, Extension> map() {
    return extensionsByRowtype;
  }

  @Override
  public List<Extension> search(String keyword) {
    List<Extension> list = new ArrayList<>();
    String finalKeyword = keyword.toLowerCase();
    extensionsByRowtype.forEach((k, v) -> {
      if (StringUtils.containsIgnoreCase(v.getSubject(), finalKeyword)) {
        list.add(v);
      }
    });
    return list;
  }

  public int updateFromRegistry() {
    int counter = 0;
    registryUpdate = new Date();

    // get a fresh copy each time
    List<URL> extensionUrls = extensionsUrlsSupplier.get();
    for (URL url : extensionUrls) {
      LOG.info("Loading #{} extension {} ...", counter + 1, url);
      install(url);
      counter++;
    }
    return counter;
  }
}
