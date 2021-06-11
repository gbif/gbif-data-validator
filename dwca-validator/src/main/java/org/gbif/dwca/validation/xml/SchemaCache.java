package org.gbif.dwca.validation.xml;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaCache {


  private static final Logger LOG = LoggerFactory.getLogger(SchemaCache.class);

  private final Map<String, Schema> cache = new HashMap<>();

  @SneakyThrows
  private Schema load(URI schema) {
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
  public Schema get(URI schema) {
    String uriStr = schema.toString();
    Schema xmlSchema = cache.get(uriStr);
    return xmlSchema != null? xmlSchema : load(schema);
  }

  @SneakyThrows
  public Schema get(String schemaUrl) {
    return get(new URI(schemaUrl));
  }

}
