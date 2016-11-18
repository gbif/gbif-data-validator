package org.gbif.validation.xml;

import org.gbif.utils.file.FileUtils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import com.sun.org.apache.xerces.internal.util.XMLCatalogResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * The main purpose of {@link XMLSchemaValidatorProvider} is to centralize the management of
 * XML schemas and {@link Validator} creation.
 */
public class XMLSchemaValidatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(XMLSchemaValidatorProvider.class);

  private static final String XML_CATALOG = "xml/catalog.xml";

  public static final String DWC_META_XML = "dwc_meta_xml";

  //TODO move this to config
  private static final String DWC_META_XML_SCHEMA = FileUtils.getClasspathFile("xml/dwc/tdwg_dwc_text.xsd").getAbsolutePath();

  private final Map<String, Schema> schemas;

  /**
   * Build a new XMLSchemaValidatorProvider instance using the default catalog
   */
  public XMLSchemaValidatorProvider() {
    this(FileUtils.getClasspathFile(XML_CATALOG).toPath());
  }

  /**
   * Build a new XMLSchemaValidatorProvider using a specific XML Catalog
   * @param xmlCatalog
   */
  public XMLSchemaValidatorProvider(Path xmlCatalog) {

    XMLCatalogResolver resolver = new XMLCatalogResolver(new String[] { xmlCatalog.toAbsolutePath().toString() });

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    schemaFactory.setResourceResolver(resolver);

    schemas = Collections.synchronizedMap(new HashMap<String, Schema>());
    try {
      schemas.put(DWC_META_XML, schemaFactory.newSchema(new StreamSource(DWC_META_XML_SCHEMA)));
    } catch (SAXException e) {
      LOG.error("Can not load XML schema", e);
    }
  }

  /**
   * Get a new instance of Validator.
   * {@link Validator} is not thread-safe, returns a new instance on each call.
   * @return a new instance of {@link Validator} or null if the key can not be found
   */
  public Validator getXmlValidator(String key) {
    if(schemas.containsKey(key)) {
      return schemas.get(key).newValidator();
    }
    return null;
  }
}
