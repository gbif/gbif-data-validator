package org.gbif.dwc.extensions;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main manager for {@link Extension}.
 *
 * Moved from https://github.com/gbif/dwca-validator3
 */
public class ExtensionManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionManager.class);

  class UpdateExtensionsTask extends TimerTask {
    @Override
    public void run() {
      LOG.info("Start updating extensions from registry");
      updateFromRegistry();
    }
  }

  private static final TermFactory TF = TermFactory.instance();
  private static final int AUTO_UPDATE_FREQUENCY_MS = 60 * 60 * 1000; //1 hour

  private final List<URL> extensionUrl;
  private final ExtensionFactory factory;
  private final HttpClient httpClient;

  private Map<Term, Extension> extensionsByRowtype = new HashMap<>();

  private final String TAXON_KEYWORD = "dwc:taxon";
  private final String OCCURRENCE_KEYWORD = "dwc:occurrence";
  private final Term OCCURRENCE_DWC = DwcTerm.Occurrence;
  private final Term SIMPLE_DWC = TF.findTerm("http://rs.tdwg.org/dwc/xsd/simpledarwincore/SimpleDarwinRecord");
  private Date registryUpdate;

  private final Timer timer = new Timer();

  public ExtensionManager(ExtensionFactory factory, HttpClient httpClient, List<URL> extensionUrl,
                          boolean autoUpdate) {
    this.factory = factory;
    this.httpClient = httpClient;
    this.extensionUrl = extensionUrl;

    if(autoUpdate) {
      // scheduled for every hour
      this.timer.scheduleAtFixedRate(new UpdateExtensionsTask(), new Date(), AUTO_UPDATE_FREQUENCY_MS);
    }else{
      //run once now
      updateFromRegistry();
    }
  }

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
  public void install(URL url) {
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


  public Map<Term, Extension> map() {
    return extensionsByRowtype;
  }

  public List<Extension> search(String keyword) {
    List<Extension> list = new ArrayList<>();
    keyword = keyword.toLowerCase();
    for (Extension e : extensionsByRowtype.values()) {
      if (StringUtils.containsIgnoreCase(e.getSubject(), keyword)) {
        list.add(e);
      }
    }
    return list;
  }

  public int updateFromRegistry() {
    int counter = 0;
    registryUpdate = new Date();

    for (URL url : extensionUrl) {
      LOG.info("Loading #{} extension {} ...", counter + 1, url);
      install(url);
      counter++;
    }

    return counter;
  }

}
