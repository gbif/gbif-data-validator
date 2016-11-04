package org.gbif.validation.xml;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * The main purpose of {@link XMLSchemaValidatorProvider} is to centralize the management of
 * XML schemas and {@link Validator} creation.
 */
public class XMLSchemaValidatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(XMLSchemaValidatorProvider.class);

  private static final String SCHEMA_LANG = "http://www.w3.org/2001/XMLSchema";
  private final SchemaFactory SCHEMA_FACTORY = SchemaFactory.newInstance(SCHEMA_LANG);

  public static final String DWC_META_XML = "dwc_meta_xml";

  //TODO move this to config
  private static final String DWC_META_XML_SCHEMA = "https://raw.githubusercontent.com/tdwg/dwc/master/text/tdwg_dwc_text.xsd";

  private final Map<String, Schema> schemas;

  //TODO implement schema cache in local folder
  //public XMLSchemaValidatorProvider(String workingFolder, Map<String, String> schemaKeyUrl) {
  public XMLSchemaValidatorProvider() {
    schemas = Collections.synchronizedMap(new HashMap());
    try {
      schemas.put(DWC_META_XML, SCHEMA_FACTORY.newSchema(new StreamSource(DWC_META_XML_SCHEMA)));
    } catch (SAXException e) {
      LOG.error("Can not load XML schema {}", e);
    }
  }

  /**
   * Get a new instance of Validator.
   * Validator are not thread-safe, get a new instance on each call.
   * @return
   */
  public Validator getXmlValidator(String key) {
    if(schemas.containsKey(key)) {
      return schemas.get(key).newValidator();
    }
    return null;
  }
}
