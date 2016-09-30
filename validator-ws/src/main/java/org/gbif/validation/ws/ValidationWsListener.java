package org.gbif.validation.ws;

import org.gbif.drupal.guice.DrupalMyBatisModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.mixin.Mixins;
import org.gbif.ws.server.guice.GbifServletListener;
import org.gbif.ws.server.guice.WsAuthModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;

public class ValidationWsListener extends GbifServletListener {



  private static final String APP_CONF_FILE = "validation.properties";

  private static class ValidationModule extends PrivateServiceModule {

    private static final String PROPERTIES_PREFIX = "validation.";

    public ValidationModule(Properties properties) {
      super(PROPERTIES_PREFIX,properties);
    }

    @Override
    protected void configureService() {
      ValidationConfiguration configuration = new ValidationConfiguration();
      configuration.setApiUrl(getProperties().getProperty(ConfKeys.API_URL_CONF_KEY));
      configuration.setWorkingDir(getProperties().getProperty(ConfKeys.WORKING_DIR_CONF_KEY));
      bind(ValidationConfiguration.class).toInstance(configuration);
      expose(ValidationConfiguration.class);
    }
  }

  public ValidationWsListener() throws IOException {
    super(PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE)), "org.gbif.validation.ws", true);
  }

  @Override
  @VisibleForTesting
  protected Injector getInjector() {
    return super.getInjector();
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new ValidationModule(properties));
    modules.add(new WsAuthModule(properties));
    modules.add(new DrupalMyBatisModule(properties));
    return modules;
  }

  @Override
  protected Map<Class<?>, Class<?>> getMixIns() {
    return Mixins.getPredefinedMixins();
  }

}
