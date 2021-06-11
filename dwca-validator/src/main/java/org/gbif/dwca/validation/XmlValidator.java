package org.gbif.dwca.validation;

import java.util.List;

import lombok.Builder;
import lombok.Data;
import org.xml.sax.SAXParseException;

public interface XmlValidator {

  @Data
  @Builder
  public static class ValidationError {

    public enum Level {
      WARNING, ERROR, FATAL
    }

    private final XmlValidator.ValidationError.Level level;

    private final SAXParseException error;

  }

  @Data
  @Builder
  public static class ValidationResult {

    private final List<XmlValidator.ValidationError> errors;

    public boolean isValid() {
      return errors.isEmpty();
    }
  }

  ValidationResult validate(String document);

}
