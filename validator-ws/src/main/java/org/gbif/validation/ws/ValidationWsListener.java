package org.gbif.validation.ws;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.common.parsers.NumberParser;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.conf.ValidatorConfiguration;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.JobServer;
import org.gbif.validation.jobserver.impl.ActorPropsSupplier;
import org.gbif.validation.jobserver.impl.FileJobStorage;
import org.gbif.validation.ws.conf.ConfKeys;
import org.gbif.validation.ws.conf.ValidationWsConfiguration;
import org.gbif.validation.ws.file.UploadedFileManager;
import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.mixin.Mixins;
import org.gbif.ws.server.guice.GbifServletListener;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server listener. Contains
 */
public class ValidationWsListener extends GbifServletListener {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationWsListener.class);

  //Default configuration file
  private static final String APP_CONF_FILE = "validation.properties";

  /**
   * Private module that installs all the required settings and exposes the service configuration.
   */
  private static class ValidationModule extends PrivateServiceModule {

    private static final String PROPERTIES_PREFIX = "validation.";

    private static final String NORMALIZER_CONF = "clb-normalizer.yaml";

    private static final int DEFAULT_SPLIT_SIZE = 10000;

    private static final int HTTP_CLIENT_TO = 60000;
    private static final int HTTP_CLIENT_THREADS = 20;
    private static final int HTTP_CLIENT_THREADS_PER_ROUTE = 20;

    private static final TypeLiteral<JobServer<ValidationResult>> JOB_SERVER_TYPE_LITERAL =
      new TypeLiteral<JobServer<ValidationResult>>(){};

    ValidationModule(Properties properties) {
      super(PROPERTIES_PREFIX,properties);
    }

    private static ValidationWsConfiguration getConfFromProperties(Properties properties){
      ValidationWsConfiguration configuration = new ValidationWsConfiguration();

      configuration.setApiUrl(properties.getProperty(ConfKeys.API_URL_CONF_KEY));
      configuration.setWorkingDir(properties.getProperty(ConfKeys.WORKING_DIR_CONF_KEY));
      configuration.setFileSplitSize(NumberUtils.toInt(properties.getProperty(ConfKeys.FILE_SPLIT_SIZE),
                                                       DEFAULT_SPLIT_SIZE));
      configuration.setApiDataValidationPath(properties.getProperty(ConfKeys.VALIDATION_API_PATH_CONF_KEY));
      configuration.setJobResultStorageDir(properties.getProperty(ConfKeys.RESULT_STORAGE_DIR_CONF_KEY));

      try {
        configuration.setExtensionDiscoveryUrl(new URL(properties.getProperty(ConfKeys.EXTENSION_DISCOVERY_URL_KEY)));
      } catch (MalformedURLException e) {
        LOG.error("Can't set ExtensionDiscoveryUrl", e);
      }

      //optional settings
      configuration.setPreserveTemporaryFiles(
              BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBoolean(properties.getProperty(ConfKeys.PRESERVE_TEMPORARY_FILES)), false));
      configuration.setGangliaHost(properties.getProperty(ConfKeys.GANGLIA_HOST));
      configuration.setGangliaPort(NumberParser.parseInteger(properties.getProperty(ConfKeys.GANGLIA_PORT)));

      return configuration;
    }

    /**
     * Creates the workingDir and the file storage directory.
     */
    private static void createWorkingDirs(ValidationWsConfiguration configuration) {
      try {
        Files.createDirectories(Paths.get(configuration.getWorkingDir()));
        Files.createDirectories(Paths.get(configuration.getJobResultStorageDir()));
      } catch (IOException ioex) {
        throw new IllegalStateException("Error creating working directories", ioex);
      }
    }

    /**
     * Creates an instance of a JobServer using  the provided configuration.
     */
    private static JobServer<ValidationResult> getJobServerInstance(ValidationWsConfiguration configuration,
                                                                    UploadedFileManager uploadedFileManager) {
      return new JobServer<>(new FileJobStorage(Paths.get(configuration.getJobResultStorageDir())),
                             buildActorPropsMapping(configuration), uploadedFileManager::cleanByKey);
    }

    @Override
    protected void configureService() {
      //get configuration settings
      ValidationWsConfiguration configuration = getConfFromProperties(getProperties());
      UploadedFileManager uploadedFileManager;
      try {
        uploadedFileManager = new UploadedFileManager(configuration.getWorkingDir());
        bind(UploadedFileManager.class).toInstance(uploadedFileManager);
      } catch (IOException e) {
        LOG.error("Can't instantiate uploadedFileManager", e);
        return;
      }

      //create required directories
      createWorkingDirs(configuration);

      //Guice bindings
      HttpUtil httpUtil = new HttpUtil(HttpUtil.newMultithreadedClient(HTTP_CLIENT_TO, HTTP_CLIENT_THREADS,
                                                                       HTTP_CLIENT_THREADS_PER_ROUTE));

      bind(HttpUtil.class).toInstance(httpUtil);
      bind(JOB_SERVER_TYPE_LITERAL).toInstance(getJobServerInstance(configuration, uploadedFileManager));
      bind(ValidationWsConfiguration.class).toInstance(configuration);

      expose(JOB_SERVER_TYPE_LITERAL);
      expose(ValidationWsConfiguration.class);
      expose(HttpUtil.class);
      expose(UploadedFileManager.class);
    }

    /**
     * Builds an instance of {@link ActorPropsSupplier} which is used by the Akka components.
     */
    private static ActorPropsSupplier buildActorPropsMapping(ValidationWsConfiguration configuration) {
      ValidatorConfiguration config = ValidatorConfiguration.builder()
              .setApiUrl(configuration.getApiUrl())
              .setNormalizerConfiguration(getNormalizerConfiguration())
              .setExtensionListURL(configuration.getExtensionDiscoveryUrl())
              .setPreserveTemporaryFiles(configuration.isPreserveTemporaryFiles())
              .setGangliaHost(configuration.getGangliaHost().orElse(null))
              .setGangliaPort(configuration.getGangliaPort().orElse(null))
              .build();

      return new ActorPropsSupplier(new EvaluatorFactory(config),
              configuration.getFileSplitSize(),
              configuration.getWorkingDir(),
              config.isPreservedTemporaryFiles());
    }

    /**
     * Reads the NormalizerConfiguration from the file NORMALIZER_CONF.
     */
    private static NormalizerConfiguration getNormalizerConfiguration() {
      try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(Thread.currentThread().getContextClassLoader().getResource(NORMALIZER_CONF),
                                NormalizerConfiguration.class);
      } catch (IOException ex) {
        throw new IllegalStateException(ex);
      }
    }
  }

  public ValidationWsListener() throws IOException {
    super(PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE)), "org.gbif.validation.ws", false);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new ValidationModule(properties));
    return modules;
  }

  /**
   * Installs predefined mixins.
   */
  @Override
  protected Map<Class<?>, Class<?>> getMixIns() {
    return Mixins.getPredefinedMixins();
  }

}
