package org.gbif.validation.evaluator;

import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionManager;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationIssue;
import org.gbif.validation.api.result.ValidationIssues;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Class to evaluate the structure of a DarwinCore Archive.
 * That includes meta.xml schema validation.
 */
class DwcaResourceStructureEvaluator implements ResourceStructureEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaResourceStructureEvaluator.class);

  private final XMLSchemaValidatorProvider xmlSchemaValidatorProvider;
  private final ExtensionManager extensionManager;

  DwcaResourceStructureEvaluator(XMLSchemaValidatorProvider xmlSchemaValidatorProvider,
                                 ExtensionManager extensionManager) {
    this.xmlSchemaValidatorProvider = xmlSchemaValidatorProvider;
    this.extensionManager = extensionManager;
  }

  @Override
  public Optional<List<ValidationResultElement>> evaluate(@NotNull DataFile dataFile) {
    Objects.requireNonNull(dataFile.getFilePath(), "DataFile filePath shall be provided");
    Objects.requireNonNull(dataFile.getSourceFileName(), "DataFile sourceFileName shall be provided");

    List<ValidationResultElement> validationResultElements = new ArrayList<>();
    try {
      Archive archive = ArchiveFactory.openArchive(dataFile.getFilePath().toFile());
      File metaXmlFile = new File(dataFile.getFilePath().toFile(), Archive.META_FN);
      if (metaXmlFile.exists()){
        try {
          getMetaXMLValidator().validate(new StreamSource(metaXmlFile.getAbsolutePath()));

          //Make sure we can read the headers
          DataFileFactory.prepareDataFile(dataFile);

          if(!archive.getExtensions().isEmpty()) {
            archive.getExtensions().forEach(
                    ext -> evaluateArchiveFile(dataFile, ext).
                            ifPresent( vre -> validationResultElements.add(vre))
            );
          }
        } catch (SAXException e) {
          validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                  EvaluationType.DWCA_META_XML_SCHEMA, e.getMessage()));
        }
      }
      else{
        validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                EvaluationType.DWCA_META_XML_NOT_FOUND, null));
      }
    } catch (IOException | UnsupportedArchiveException uaEx) {
      LOG.debug("Can't evaluate Dwca", uaEx);
      validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
              EvaluationType.DWCA_UNREADABLE, uaEx.getMessage()));
    }
    return validationResultElements.isEmpty() ? Optional.empty() : Optional.of(validationResultElements);
  }

  private Optional<ValidationResultElement> evaluateArchiveFile(DataFile dataFile, ArchiveFile archiveFile) {
    List<ValidationIssue> validationIssues = new ArrayList<>();
    // registered extension?
    Extension ext = extensionManager.get(archiveFile.getRowType());
    if (ext != null) {
      //check for required fields
      ext.getProperties().stream()
              .filter(ep -> ep.isRequired() && !archiveFile.hasTerm(ep.getQualname()))
              .forEach(ep -> validationIssues.add(ValidationIssues.withRelatedData(
                      EvaluationType.REQUIRED_TERM_MISSING, ep.getQualname())));

      //check for possible unknown terms inside a known Extension
      archiveFile.getFields().keySet().stream()
              .filter(t -> !ext.hasProperty(t))
              .forEach(t -> validationIssues.add(ValidationIssues.withRelatedData(
                      EvaluationType.UNKNOWN_TERM, t.qualifiedName())));
    } else {
      validationIssues.add(ValidationIssues.withRelatedData(
              EvaluationType.UNKNOWN_ROWTYPE, archiveFile.getRowType().toString()));
    }
    return validationIssues.isEmpty() ? Optional.empty() :
            Optional.of(new ValidationResultElement(archiveFile.getLocationFile().getName(), null, DwcFileType.METADATA,
                    archiveFile.getRowType(), validationIssues));
  }

  private Validator getMetaXMLValidator() {
    return xmlSchemaValidatorProvider.getXmlValidator(XMLSchemaValidatorProvider.DWC_META_XML);
  }


}
