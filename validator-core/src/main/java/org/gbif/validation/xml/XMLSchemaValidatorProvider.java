package org.gbif.validation.xml;

import org.gbif.utils.file.FileUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import com.sun.org.apache.xerces.internal.util.XMLCatalogResolver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * The main purpose of {@link XMLSchemaValidatorProvider} is to centralize the management of
 * XML schemas and {@link Validator} creation.
 */
public class XMLSchemaValidatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(XMLSchemaValidatorProvider.class);

  public static final String DWC_META_XML = "dwc_meta_xml";
  public static final String GBIF_EML = "gbif_eml";

  //TODO move this to config and get Stream
  private static final String DWC_META_XML_SCHEMA = "xml/dwc/tdwg_dwc_text.xsd";
  private static final String GBIF_EML_SCHEMA = "http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd";

  private final Map<String, Schema> schemas;

  /**
   * Build a new XMLSchemaValidatorProvider instance that will NOT use a XMLCatalog
   */
  public XMLSchemaValidatorProvider() {
    this(Optional.empty());
  }

  /**
   * Build a new XMLSchemaValidatorProvider using optionally a XML Catalog.
   *
   * @param xmlCatalog path to XMLCatalog
   */
  public XMLSchemaValidatorProvider(Optional<String> xmlCatalog) {

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    xmlCatalog.ifPresent(xc -> schemaFactory.setResourceResolver(new XMLCatalogResolver(new String[]{xc})));

    schemas = Collections.synchronizedMap(new HashMap<>());
    try {
      schemas.put(DWC_META_XML, schemaFactory.newSchema(getStreamSource(DWC_META_XML_SCHEMA)));
      schemas.put(GBIF_EML, schemaFactory.newSchema(getStreamSource(GBIF_EML_SCHEMA)));
    } catch (SAXException | IOException e) {
      LOG.error("Can not load XML schema", e);
    }
  }

  /**
   * Get a {@link StreamSource} instance for the provided path.
   * If the path represents an HTTP adress it will be loaded from there otherwise, it will be loaded from the
   * classpath.
   *
   * @param path
   * @return
   * @throws IOException
   */
  private static Source getStreamSource(String path) throws IOException {
    return StringUtils.startsWith(path, "http")? new StreamSource(path) :
                                                 new StreamSource(FileUtils.classpathStream(path));
  }

  /**
   * Get a new instance of Validator.
   * {@link Validator} is not thread-safe, returns a new instance on each call.
   * @return a new instance of {@link Validator} or null if the key can not be found
   */
  public Validator getXmlValidator(String key) {
    return schemas.containsKey(key)? schemas.get(key).newValidator() : null;
  }
}
