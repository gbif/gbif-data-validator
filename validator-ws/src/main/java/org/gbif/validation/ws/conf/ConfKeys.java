package org.gbif.validation.ws.conf;

/**
 * Configuration keys used in properties file.
 */
public final class ConfKeys {

  public static final String API_URL_CONF_KEY = "apiUrl";
  public static final String EXTENSION_DISCOVERY_URL_KEY = "extensionDiscoveryUrl";
  public static final String VALIDATION_API_PATH_CONF_KEY = "apiDataValidationPath";
  public static final String WORKING_DIR_CONF_KEY = "workingDir";
  public static final String FILE_SPLIT_SIZE = "fileSplitSize";
  public static final String RESULT_STORAGE_DIR_CONF_KEY = "jobResultStorageDir";

  /**
   * Private constructor.
   */
  private ConfKeys() {
    //empty constructor
  }
}
