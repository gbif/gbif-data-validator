package org.gbif.validation.evaluator;

import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Class to evaluate the structure of an EML file.
 * That includes xml schema validation.
 */
class EmlResourceStructureEvaluator implements ResourceStructureEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(EmlResourceStructureEvaluator.class);

  private final XMLSchemaValidatorProvider xmlSchemaValidatorProvider;

  EmlResourceStructureEvaluator(XMLSchemaValidatorProvider xmlSchemaValidatorProvider) {
    this.xmlSchemaValidatorProvider = xmlSchemaValidatorProvider;
  }

  @Override
  public Optional<ValidationResultElement> evaluate(@NotNull DataFile dataFile) {

    Objects.requireNonNull(dataFile.getFilePath(), "DataFile filePath shall be provided");
    Objects.requireNonNull(dataFile.getSourceFileName(), "DataFile sourceFileName shall be provided");

    try {
      Archive archive = ArchiveFactory.openArchive(dataFile.getFilePath().toFile());
      File datasetMetadataFile = archive.getMetadataLocationFile();
      if (datasetMetadataFile.exists()) {
        try {
          getMetaXMLValidator().validate(new StreamSource(datasetMetadataFile.getAbsolutePath()));
        } catch (SAXException e) {
          return Optional.of(buildResult(dataFile.getSourceFileName(), EvaluationType.EML_GBIF_SCHEMA, e.getMessage()));
        }
      }
      else{
        return Optional.of(buildResult(dataFile.getSourceFileName(), EvaluationType.EML_NOT_FOUND, null));
      }
    } catch (IOException | UnsupportedArchiveException uaEx) {
      LOG.debug("Can't evaluate EML file", uaEx);
      //this is a tricky one since it is not really possible to know if the error is coming from the EML
      return Optional.of(buildResult(dataFile.getSourceFileName(), EvaluationType.EML_NOT_FOUND, uaEx.getMessage()));
    }

    return Optional.empty();
  }

  private Validator getMetaXMLValidator() {
    return xmlSchemaValidatorProvider.getXmlValidator(XMLSchemaValidatorProvider.GBIF_EML);
  }

  /**
   *
   * @param sourceFilename
   * @param type
   * @param msg
   * @return
   */
  private static ValidationResultElement buildResult(String sourceFilename, EvaluationType type, String msg){
    return ValidationResultElement.onException(sourceFilename, type, msg);
  }
}
