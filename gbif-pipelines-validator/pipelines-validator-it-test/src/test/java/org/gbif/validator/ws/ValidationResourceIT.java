package org.gbif.validator.ws;

import org.gbif.validator.it.EmbeddedDataBaseInitializer;
import org.gbif.validator.it.ValidatorWsItConfiguration;
import org.gbif.validator.ws.resource.ValidationResource;

import java.nio.file.Path;
import java.util.stream.Stream;

import io.zonky.test.db.postgres.embedded.LiquibasePreparer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/** Base class for IT tests that initializes data sources and basic security settings. */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
  classes = ValidatorWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = {ValidationResourceIT.ContextInitializerItTests.class})
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DirtiesContext
public class ValidationResourceIT {

  //Directory used as temporary space to upload files
  @TempDir
  static Path workingDirectory;

  //Directory used as files store
  @TempDir
  static Path storeDirectory;

  private final ValidationResource validationResource;

  @Autowired
  public ValidationResourceIT(ValidationResource validationResource) {
    this.validationResource = validationResource;
  }

  @Test
  public void test() {
    Assertions.assertNotNull(validationResource);
  }

  static class ContextInitializerItTests
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    public static final String LIQUIBASE_MASTER_FILE = "liquibase/master.xml";

    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {

      EmbeddedDataBaseInitializer database =
        new EmbeddedDataBaseInitializer(
          LiquibasePreparer.forClasspathLocation(LIQUIBASE_MASTER_FILE));

      TestPropertyValues.of(
        Stream.of(dbTestPropertyPairs(database)).toArray(String[]::new))
        .applyTo(configurableApplicationContext.getEnvironment());

    }

    /** Creates the registry datasource settings from the embedded database. */
    String[] dbTestPropertyPairs(EmbeddedDataBaseInitializer database) {
      return new String[] {
        "validation.datasource.url=jdbc:postgresql://localhost:"
        + database.getConnectionInfo().getPort()
        + "/"
        + database.getConnectionInfo().getDbName(),
        "validation.datasource.username=" + database.getConnectionInfo().getUser(),
        "validation.datasource.password=" +
        "upload.workingDirectory=" + workingDirectory.toString() +
        "upload.maxUploadSize=3145728" +
        "storePath=" + storeDirectory.toString()
      };
    }
  }
}
