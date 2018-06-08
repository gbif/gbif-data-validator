package org.gbif.validation.evaluator;

import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionManager;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveField;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.TermWithinRowType;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationIssue;
import org.gbif.validation.api.result.ValidationIssues;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Class to evaluate the structure of a DarwinCore Archive.
 * That includes meta.xml schema validation, checks for mandatory and unknown terms.
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
      Archive archive = DwcFiles.fromLocationSkipValidation(dataFile.getFilePath());
      File metaXmlFile = new File(dataFile.getFilePath().toFile(), Archive.META_FN);
      if (metaXmlFile.exists()) {
        try {
          getMetaXMLValidator().validate(new StreamSource(metaXmlFile.getAbsolutePath()));

          evaluateArchiveFile(archive.getCore()).ifPresent(validationResultElements::add);
          if (!archive.getExtensions().isEmpty()) {
            archive.getExtensions().forEach(
                    ext -> evaluateArchiveFile(ext).
                            ifPresent(validationResultElements::add)
            );
          }
        } catch (SAXException e) {
          validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                  EvaluationType.DWCA_META_XML_SCHEMA, e.getMessage()));
        }
      } else {
        validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                EvaluationType.DWCA_META_XML_NOT_FOUND, null));
      }
    } catch (IOException | UnsupportedArchiveException uaEx) {
      LOG.info("Can't evaluate Dwca", uaEx);
      validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
              EvaluationType.DWCA_UNREADABLE, uaEx.getMessage()));
    }
    return validationResultElements.isEmpty() ? Optional.empty() : Optional.of(validationResultElements);
  }

  private Optional<ValidationResultElement> evaluateArchiveFile(ArchiveFile archiveFile) {
    List<ValidationIssue> validationIssues = new ArrayList<>();
    // registered extension?
    Extension ext = extensionManager.get(archiveFile.getRowType());
    if (ext != null) {

      // Check for duplicated terms
      // +1 on RHS is because that method doesn't include the id field.
      if (archiveFile.getHeader().stream().mapToInt(l -> l.size()).sum() != archiveFile.getRawArchiveFields().size() + 1) {
        getDuplicatedTerm(archiveFile).forEach(
                t -> validationIssues.add(ValidationIssues.withRelatedData(
                        EvaluationType.DUPLICATED_TERM, TermWithinRowType.of(ext.getRowType(), t))));
      }

      //check for required fields
      ext.getProperties().stream()
              .filter(ep -> ep.isRequired() && !archiveFile.hasTerm(ep.getQualname()))
              .forEach(ep -> validationIssues.add(ValidationIssues.withRelatedData(
                      EvaluationType.REQUIRED_TERM_MISSING, TermWithinRowType.of(ext.getRowType(), ep))));

      //check for possible unknown terms inside a known Extension
      archiveFile.getFields().keySet().stream()
              .filter(t -> !ext.hasProperty(t))
              .forEach(t -> validationIssues.add(ValidationIssues.withRelatedData(
                      EvaluationType.UNKNOWN_TERM, TermWithinRowType.of(ext.getRowType(), t))));
    } else {
      validationIssues.add(ValidationIssues.withRelatedData(
              EvaluationType.UNKNOWN_ROWTYPE, TermWithinRowType.ofRowType(archiveFile.getRowType())));
    }
    return validationIssues.isEmpty() ? Optional.empty() :
            Optional.of(ValidationResultElement.forMetaDescriptor(Archive.META_FN, validationIssues));
  }

  /**
   * Get a {@link Set} of all {@link Term} that are not used uniquely.
   *
   * @param archiveFile
   * @return {@link Set} of all {@link Term} that are used more than once or empty {@link Set} if none.
   */
  private Set<Term> getDuplicatedTerm(ArchiveFile archiveFile) {
    Set<Term> uniqueTerms = new HashSet<>();
    Set<Term> nonUniqueTerms = new HashSet<>();
    for(ArchiveField headerTerm : archiveFile.getRawArchiveFields()) {
      if(!uniqueTerms.add(headerTerm.getTerm())) {
        nonUniqueTerms.add(headerTerm.getTerm());
      }
    }
    return nonUniqueTerms;
  }

  private Validator getMetaXMLValidator() {
    return xmlSchemaValidatorProvider.getXmlValidator(XMLSchemaValidatorProvider.DWC_META_XML);
  }

}
