package org.gbif.validation.ws.resources;

import org.gbif.validation.api.model.FileFormat;

import java.nio.charset.Charset;

/**
 * Parameters and their default values of the data validation service.
 */
public class WsValidationParams<T> {

  public static final WsValidationParams<String>
    FILE = new WsValidationParams<>("file", null);
  public static final WsValidationParams<FileFormat> FORMAT = new WsValidationParams<>("format", FileFormat.TABULAR);
  public static final WsValidationParams<Charset>
    ENCODING = new WsValidationParams<>("encondig", Charset.forName("UTF-8"));
  public static final WsValidationParams<String>
    LINES_TERMINATED_BY = new WsValidationParams<>("linesTerminatedBy", "\r\n");
  public static final WsValidationParams<Character>
    FIELDS_TERMINATED_BY = new WsValidationParams<>("fieldsTerminatedBy", ',');
  public static final WsValidationParams<Character> FIELDS_ENCLOSED_BY = new WsValidationParams<>("fieldsEnclosedBy", '"');
  public static final WsValidationParams<String>
    DATE_FORMAT = new WsValidationParams<>("dateFormat", "yyyy-MM-dd'T'HH:mm'Z'");
  public static final WsValidationParams<Character> DECIMAL_SEPARATOR = new WsValidationParams<>("decimalSeparator", ',');
  public static final WsValidationParams<Boolean> HAS_HEADERS = new WsValidationParams<>("hasHeaders", Boolean.TRUE);


  private final String param;

  private final T defaultValue;

  /**
   * Private full constructor.
   */
  private WsValidationParams(String param, T defaultValue){
    this.param = param;
    this.defaultValue = defaultValue;
  }

  /**
   * Parameter name.
   */
  public String getParam() {
    return param;
  }

  /**
   * Default value of this parameter.
   */
  public T getDefaultValue(){
    return defaultValue;
  }
}
