package org.gbif.dwca.validation.xml;

import org.gbif.dwca.validation.XmlSchemaValidator;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;

@ExtendWith(MockServerExtension.class)
public class SchemaValidatorFactoryTest {

  private ClientAndServer clientAndServer;

  private SchemaValidatorFactory schemaValidatorFactory;

  @SneakyThrows
  public SchemaValidatorFactoryTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    this.schemaValidatorFactory = new SchemaValidatorFactory(testPath(clientAndServer, "/eml.xsd"));
  }

  @Test
  public void testLoadSchema() {
    //Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);

    //Gets the test schema
    XmlSchemaValidator emlSchemaValidator = schemaValidatorFactory.newValidator(testPath(clientAndServer, "/eml.xsd"));

    //The schema loads successfully
    assertNotNull(emlSchemaValidator);
  }

  @Test
  public void testNotExistingSchema() {
    //Gets the test schema
    XmlSchemaValidator emlSchemaValidator = schemaValidatorFactory.newValidator(testPath(clientAndServer, "/notExist.xsd"));

    //The schema loads successfully
    assertNull(emlSchemaValidator);
  }
}
