package org.gbif.validation.ws;

/**
 * Configuration keys used in properties file.
 */
public final class ConfKeys {

  public static final String API_URL_CONF_KEY = "apiUrl";
  public static final String WORKING_DIR_CONF_KEY = "workingDir";
  public static final String FILE_SPLIT_SIZE = "fileSplitSize";

  public static final String LIVY_URL = "livyUrl";
  public static final String LIVY_JARS = "livyJars";

  /**
   * Private constructor.
   */
  private ConfKeys() {
    //empty constructor
  }
}
