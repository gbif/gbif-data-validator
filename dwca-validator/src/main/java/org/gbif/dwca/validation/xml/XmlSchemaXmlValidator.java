package org.gbif.dwca.validation.xml;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;

import org.gbif.dwca.validation.XmlValidator;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

@Data
@Builder
public class XmlSchemaXmlValidator implements XmlValidator {

  private static final Logger LOG = LoggerFactory.getLogger(XmlSchemaXmlValidator.class);

  private final SchemaCache schemaCache;

  private final String schemaUrl;

  private Validator newValidator(Schema schema) {
    Validator validator = schema.newValidator();
    validator.setErrorHandler(new CollectorErrorHandler());
    return validator;
  }

  @SneakyThrows
  @Override
  public ValidationResult validate(String document) {
    Validator validator = newValidator(schemaCache.get(schemaUrl));
    validator.validate(new StreamSource(new StringReader(document)));
    return ValidationResult.builder()
            .errors(((CollectorErrorHandler)validator.getErrorHandler()).getErrors())
            .build();
  }

  /**
   * Error handler that collects all possible errors, it only stops when a FATAL error is discovered.
   */
  @Data
  public static class CollectorErrorHandler implements ErrorHandler {

    private final List<ValidationError> errors = new ArrayList<>();

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      errors.add(ValidationError.builder()
                   .level(ValidationError.Level.WARNING)
                   .error(exception)
                   .build());
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      errors.add(ValidationError.builder()
                   .level(ValidationError.Level.ERROR)
                   .error(exception)
                   .build());
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      errors.add(ValidationError.builder()
                   .level(ValidationError.Level.FATAL)
                   .error(exception)
                   .build());
      throw exception;
    }
  }
}
