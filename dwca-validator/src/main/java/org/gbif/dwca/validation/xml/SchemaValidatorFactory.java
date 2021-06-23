package org.gbif.dwca.validation.xml;

import org.gbif.dwca.validation.XmlSchemaValidator;

import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

public class SchemaValidatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaValidatorFactory.class);

  private final Map<String, Schema> cache = new HashMap<>();

  public SchemaValidatorFactory() {
    //Nothing
  }

  @SneakyThrows
  public SchemaValidatorFactory(String ... preLoadedSchemaLocations) {
    for (String schema : preLoadedSchemaLocations) {
      load(new URI(schema));
    }
  }

  @SneakyThrows
  public Schema load(URI schema) {
    LOG.info("Loading xml schema from {}", schema);
    // define the type of schema - we use W3C:
    // resolve validation driver:
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    // create schema by reading it from gbif online resources:
    Schema xmlSchema = factory.newSchema(schema.toURL());
    cache.put(schema.toString(), xmlSchema);
    return xmlSchema;
  }

  @SneakyThrows
  public XmlSchemaValidator newValidator(String schemaUrl) {
    return  Optional.ofNullable(schemaUrl)
              .map(cache::get)
              .map(val -> XmlSchemaValidatorImpl.builder().schema(val).build())
              .orElseThrow(() -> new IllegalArgumentException(schemaUrl + "XML schema not supported"));
  }

  @SneakyThrows
  public XmlSchemaValidator newValidatorFromDocument(String xmlDocument) {
    return  Optional.ofNullable(getSchema(xmlDocument))
              .map(this::newValidator)
              .orElseThrow(() -> new IllegalArgumentException("schemaLocation not found in document"));
  }

  /**
   * Extracts the schemaLocation from a xml document.
   * The schema location it is expected to be as an attribute of the first element in the XML document.
   * For example:
   * <pre>
   *   <?xml version="1.0" encoding="utf-8"?>
   * <eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   *          xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd"
   *          ...
   * </pre>
   */
  @SneakyThrows
  private String getSchema(String xmlDocument) {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setValidating(false);
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(new InputSource(new StringReader(xmlDocument)));
      if (doc.hasChildNodes()) {
        Node node = doc.getChildNodes().item(0).getAttributes().getNamedItem("xsi:schemaLocation");
        if (node != null) {
          String[] schemaLocation = node.getTextContent().split(" ");
          if(schemaLocation.length == 2) {
            return schemaLocation[1];
          }
        }
      }
      return null;
  }

}
