package org.gbif.validation.evaluator;

import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Class to evaluate the structure of a DarwinCore Archive.
 * That includes meta.xml schema validation.
 */
public class DwcaResourceStructureEvaluator implements ResourceStructureEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaResourceStructureEvaluator.class);

  private final XMLSchemaValidatorProvider xmlSchemaValidatorProvider;

  public DwcaResourceStructureEvaluator(XMLSchemaValidatorProvider xmlSchemaValidatorProvider) {
    this.xmlSchemaValidatorProvider = xmlSchemaValidatorProvider;
  }

  @Override
  public Optional<ValidationResultElement> evaluate(Path dwcFolder, String sourceFilename) {
    try {
      ArchiveFactory.openArchive(dwcFolder.toFile());
      File metaXmlFile = new File(dwcFolder.toFile(), Archive.META_FN);
      if (metaXmlFile.exists()){
        try {
          getMetaXMLValidator().validate(new StreamSource(metaXmlFile.getAbsolutePath()));
        } catch (SAXException e) {
          return Optional.of(buildResult(sourceFilename, EvaluationType.DWCA_META_XML_SCHEMA, e.getMessage()));
        }
      }
      else{
        return Optional.of(buildResult(sourceFilename, EvaluationType.DWCA_META_XML_NOT_FOUND, null));
      }
    } catch (IOException | UnsupportedArchiveException uaEx) {
      LOG.debug("Can't evaluate Dwca", uaEx);
      return Optional.of(buildResult(sourceFilename, EvaluationType.DWCA_UNREADABLE, uaEx.getMessage()));
    }
    return Optional.empty();
  }

  private Validator getMetaXMLValidator() {
    return xmlSchemaValidatorProvider.getXmlValidator(XMLSchemaValidatorProvider.DWC_META_XML);
  }

  private static ValidationResultElement buildResult(String sourceFilename, EvaluationType type, String msg){
    return ValidationResultBuilders.DefaultValidationResultElementBuilder
            .of(sourceFilename).addExceptionResultDetails(type, msg).build();
  }

}
