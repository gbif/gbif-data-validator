package org.gbif.ws.server.provider;

import org.gbif.occurrence.validation.api.DataFileDescriptor;
import org.gbif.occurrence.validation.api.FileFormat;
import org.gbif.validation.ws.WsValidationParams;

import java.nio.charset.Charset;
import java.util.function.Function;

import com.sun.jersey.multipart.FormDataMultiPart;

import static org.gbif.validation.ws.WsValidationParams.FORMAT;
import static org.gbif.validation.ws.WsValidationParams.ENCODING;
import static org.gbif.validation.ws.WsValidationParams.DECIMAL_SEPARATOR;
import static org.gbif.validation.ws.WsValidationParams.FIELDS_ENCLOSED_BY;
import static org.gbif.validation.ws.WsValidationParams.FIELDS_TERMINATED_BY;
import static org.gbif.validation.ws.WsValidationParams.HAS_HEADERS;
import static org.gbif.validation.ws.WsValidationParams.LINES_TERMINATED_BY;
import static org.gbif.validation.ws.WsValidationParams.DATE_FORMAT;

/**
 * Utility class to transform form parameters into DataFileDescriptor instances.
 */
public class DataFileDescriptorProvider {

  /**
   * Default private constructor.
   */
  private DataFileDescriptorProvider() {
    //empty block
  }

  /**
   * Creates an instance of DataFileDescriptor from the form data.
   */
  public static DataFileDescriptor getValue(FormDataMultiPart  formDataMultiPart) {
    DataFileDescriptor dataFileDescriptor = new DataFileDescriptor();

    dataFileDescriptor.setFormat(orElse(FORMAT, formDataMultiPart, FORMAT.getDefaultValue()));
    dataFileDescriptor.setEncoding(orElse(ENCODING, formDataMultiPart, ENCODING.getDefaultValue()));
    dataFileDescriptor.setFieldsEnclosedBy(orElse(FIELDS_ENCLOSED_BY, formDataMultiPart,
                                                  FIELDS_ENCLOSED_BY.getDefaultValue()));
    dataFileDescriptor.setFieldsTerminatedBy(orElse(FIELDS_TERMINATED_BY, formDataMultiPart,
                                                    FIELDS_TERMINATED_BY.getDefaultValue()));
    dataFileDescriptor.setLinesTerminatedBy(orElse(LINES_TERMINATED_BY, formDataMultiPart,
                                                   LINES_TERMINATED_BY.getDefaultValue()));
    dataFileDescriptor.setHasHeaders(orElse(HAS_HEADERS, formDataMultiPart, HAS_HEADERS.getDefaultValue()));
    dataFileDescriptor.setDateFormat(orElse(DATE_FORMAT, formDataMultiPart, DATE_FORMAT.getDefaultValue()));
    dataFileDescriptor.setDecimalSeparator(orElse(DECIMAL_SEPARATOR, formDataMultiPart,
                                                  DECIMAL_SEPARATOR.getDefaultValue()));
    return dataFileDescriptor;
  }

  private static Character orElse(WsValidationParams<Character> param, FormDataMultiPart formDataMultiPart,
                                  Character defaultValue) {
    return orElse(param, formDataMultiPart, defaultValue, value -> {return value.charAt(0);});
  }

  private static String orElse(WsValidationParams<String> param, FormDataMultiPart formDataMultiPart,
                               String defaultValue) {
    return orElse(param, formDataMultiPart, defaultValue, value -> {return value;});
  }

  private static FileFormat orElse(WsValidationParams<FileFormat> param, FormDataMultiPart formDataMultiPart,
                                   FileFormat defaultValue) {
    return orElse(param, formDataMultiPart, defaultValue,
                     value -> {return FileFormat.valueOf(value.toUpperCase());});
  }

  private static Boolean orElse(WsValidationParams<Boolean> param, FormDataMultiPart formDataMultiPart,
                                Boolean defaultValue) {
    return orElse(param, formDataMultiPart, defaultValue, value -> {return Boolean.valueOf(value.toUpperCase());});
  }

  private static Charset orElse(WsValidationParams<Charset> param, FormDataMultiPart formDataMultiPart,
                                Charset defaultValue) {
    return orElse(param, formDataMultiPart, defaultValue, value -> {return Charset.forName(value);});
  }

  private static <T> T orElse(WsValidationParams<T> param, FormDataMultiPart formDataMultiPart,
                              T defaultValue, Function<String,T> toString) {
    if(formDataMultiPart.getField(param.getParam()) != null) {
      String value = formDataMultiPart.getField(param.getParam()).getValue();
      if (value != null) {
        return toString.apply(value);
      }
    }
    return defaultValue;
  }
}
