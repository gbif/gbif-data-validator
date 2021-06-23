package org.gbif.dwca.validation.xml;

import org.gbif.dwca.validation.XmlSchemaValidator;

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
public class XmlSchemaValidatorTest {

  private ClientAndServer clientAndServer;

  private SchemaValidatorFactory schemaValidatorFactory;

  @SneakyThrows
  public XmlSchemaValidatorTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    this.schemaValidatorFactory = new SchemaValidatorFactory();
    //Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);
  }

  private XmlSchemaValidator getEmlValidator() {
    return schemaValidatorFactory.newValidator(testPath(clientAndServer, "/eml.xsd"));
  }

  @Test
  public void validXmlTest() {
    XmlSchemaValidator emlValidator = getEmlValidator();
    XmlSchemaValidator.ValidationResult result = emlValidator.validate(readTestFile("/xml/ebird-eml.xml"));
    assertTrue(result.isValid());
  }

  @Test
  public void invalidXmlTest() {
    XmlSchemaValidator emlValidator = getEmlValidator();
    XmlSchemaValidator.ValidationResult result = emlValidator.validate(readTestFile("/xml/invalid-ebird-eml.xml"));
    assertFalse(result.isValid());
    assertTrue(result.getErrors().size() > 0);
  }
}
