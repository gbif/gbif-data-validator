package org.gbif.dwca.validation.xml;

import org.gbif.dwca.validation.XmlValidator;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.readTestFile;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;

/**
 * XmlSchemaXmlValidator tests.
 */
@ExtendWith(MockServerExtension.class)
public class XmlSchemaXmlValidatorTest {

  private ClientAndServer clientAndServer;

  private SchemaCache schemaCache;

  @SneakyThrows
  public XmlSchemaXmlValidatorTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    this.schemaCache = new SchemaCache();
    //Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);
  }

  private XmlSchemaXmlValidator getEmlValidator() {
    return XmlSchemaXmlValidator.builder()
            .schemaUrl(testPath(clientAndServer, "/eml.xsd"))
            .schemaCache(schemaCache)
            .build();
  }

  @Test
  public void validXmlTest() {
    XmlSchemaXmlValidator emlValidator = getEmlValidator();
    XmlValidator.ValidationResult result = emlValidator.validate(readTestFile("/xml/ebird-eml.xml"));
    assertTrue(result.isValid());
  }

  @Test
  public void invalidXmlTest() {
    XmlSchemaXmlValidator emlValidator = getEmlValidator();
    XmlValidator.ValidationResult result = emlValidator.validate(readTestFile("/xml/invalid-ebird-eml.xml"));
    assertFalse(result.isValid());
    assertTrue(result.getErrors().size() > 0);
  }
}
