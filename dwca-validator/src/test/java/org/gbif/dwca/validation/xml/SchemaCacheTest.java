package org.gbif.dwca.validation.xml;

import javax.xml.validation.Schema;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.xml.sax.SAXParseException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;

@ExtendWith(MockServerExtension.class)
public class SchemaCacheTest {

  private ClientAndServer clientAndServer;

  private SchemaCache schemaCache;

  @SneakyThrows
  public SchemaCacheTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    this.schemaCache = new SchemaCache();
  }

  @Test
  public void testLoadSchema() {
    //Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);

    //Gets the test schema
    Schema emlSchema = schemaCache.get(testPath(clientAndServer, "/eml.xsd"));

    //The schema loads successfully
    assertNotNull(emlSchema);
  }

  @Test
  public void testNotExistingSchema() {
    assertThrows(SAXParseException.class, () -> schemaCache.get(testPath(clientAndServer, "/notExist.xsd")));
  }
}
