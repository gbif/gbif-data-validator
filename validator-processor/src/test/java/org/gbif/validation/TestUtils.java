package org.gbif.validation;

import org.gbif.dwc.extensions.ExtensionManager;
import org.gbif.dwc.extensions.ExtensionManagerFactory;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.HttpClient;

/**
 *
 */
public class TestUtils {
  public static final File XML_CATALOG = FileUtils.getClasspathFile("xml/xml-catalog.xml");
  public static final String EXT_URL = "http://rs.gbif.org/extension/gbif/1.0/description.xml";

  public static final HttpClient HTTP_CLIENT = HttpUtil.newMultithreadedClient(6000, 2, 1);

  //This ExtensionManager only servers 1 extension (Description)
  public static final ExtensionManager EXTENSION_MANAGER;

  static {
    ExtensionManager tmp = null;
    try {
      List<URL> testExtList = new ArrayList<>();
      testExtList.add(new URL(EXT_URL));
      tmp = ExtensionManagerFactory.buildExtensionManager(HTTP_CLIENT, testExtList, false);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    EXTENSION_MANAGER = tmp;
  }

  public TestUtils() throws MalformedURLException {
  }

}
