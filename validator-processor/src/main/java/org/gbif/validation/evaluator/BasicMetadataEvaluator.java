package org.gbif.validation.evaluator;

import org.gbif.api.model.registry.Dataset;
import org.gbif.registry.metadata.contact.ContactAdapter;
import org.gbif.registry.metadata.parse.DatasetParser;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.MetadataEvaluator;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationIssue;
import org.gbif.validation.api.result.ValidationIssues;
import org.gbif.validation.api.result.ValidationResultElement;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic implementation of {@link MetadataEvaluator} that evaluate the content of the metadata document.
 *
 */
class BasicMetadataEvaluator implements MetadataEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(BasicMetadataEvaluator.class);
  private static final int MIN_TITLE_LENGTH = 10;
  private static final int MIN_DESCRIPTION_LENGTH = 25;

  /**
   * Only runs if the metadata file exists, otherwise it returns
   * @param dwcDataFile
   * @return
   */
  @Override
  public Optional<List<ValidationResultElement>> evaluate(DwcDataFile dwcDataFile) {

    List<ValidationResultElement> validationResultElements = new ArrayList<>();
    List<ValidationIssue> validationIssues = new ArrayList<>();
    if(dwcDataFile.getMetadataFilePath().isPresent()) {
      Path metadataFilePath = dwcDataFile.getMetadataFilePath().get();
      try {
        InputStream is = FileUtils.getInputStream(metadataFilePath.toFile());
        Dataset dataset = DatasetParser.build(is);

        List<Function<Dataset, Optional<ValidationIssue>>> datasetEvalChain = Arrays.asList(
                BasicMetadataEvaluator::evaluateTitle,
                BasicMetadataEvaluator::evaluateLicense,
                BasicMetadataEvaluator::evaluateDescription,
                BasicMetadataEvaluator::evaluateContact);

        datasetEvalChain.forEach( eval -> eval.apply(dataset).ifPresent(validationIssues::add));
      } catch (IOException ex) {
        LOG.warn("IOException from BasicMetadataEvaluator is unexpected.", ex);
        validationIssues.add(ValidationIssues.withException(EvaluationType.UNHANDLED_ERROR, ex.getMessage()));
      }
    }

    if(!validationIssues.isEmpty()) {
      validationResultElements.add(
              new ValidationResultElement(
                      dwcDataFile.getMetadataFilePath().map( p -> p.getFileName().toString()).orElse(""),
                      null, DwcFileType.METADATA,
                      null, validationIssues));
    }

    return validationResultElements.isEmpty() ? Optional.empty() : Optional.of(validationResultElements);
  }


  private static Optional<ValidationIssue> evaluateTitle(Dataset dataset) {
    if (StringUtils.isBlank(dataset.getTitle()) || dataset.getTitle().length() < MIN_TITLE_LENGTH) {
      return Optional.of(
              ValidationIssues.withEvaluationTypeOnly(
                      EvaluationType.TITLE_MISSING_OR_TOO_SHORT));
    }
    return Optional.empty();
  }

  private static Optional<ValidationIssue> evaluateLicense(Dataset dataset) {
    if (dataset.getLicense() == null) {
      return Optional.of(
              ValidationIssues.withEvaluationTypeOnly(
                      EvaluationType.LICENSE_MISSING_OR_UNKNOWN));
    }
    return Optional.empty();
  }

  private static Optional<ValidationIssue> evaluateDescription(Dataset dataset) {
    if (StringUtils.isBlank(dataset.getDescription()) || dataset.getDescription().length() < MIN_DESCRIPTION_LENGTH) {
      return Optional.of(
              ValidationIssues.withEvaluationTypeOnly(
                      EvaluationType.DESCRIPTION_MISSING_OR_TOO_SHORT));
    }
    return Optional.empty();
  }

  private static Optional<ValidationIssue> evaluateContact(Dataset dataset) {
    //TODO use the same method than regsitry-metadata to get contacts
    if(dataset.getContacts() == null || dataset.getContacts().isEmpty()) {
      return Optional.of(
              ValidationIssues.withEvaluationTypeOnly(
                      EvaluationType.RESOURCE_CREATOR_MISSING_OR_INCOMPLETE));
    }

    ContactAdapter contactAdapter = new ContactAdapter(dataset.getContacts());
    if (contactAdapter.getCreators().isEmpty()) {
      return Optional.of(
              ValidationIssues.withEvaluationTypeOnly(
                      EvaluationType.RESOURCE_CREATOR_MISSING_OR_INCOMPLETE));
    }


    //we need to check if all "creators" have a first and last name.
//    List<Contact> contactsWithoutCompleteName = contactAdapter.getCreators().stream()
//            .filter( cnt -> StringUtils.isBlank(cnt.getFirstName()) || StringUtils.isBlank(cnt.getLastName()))
//            .collect(Collectors.toList());
//
//    if(!contactsWithoutCompleteName.isEmpty()){
//
//      //String authorNames = CitationGenerator.
//
//      //ValidationIssues.withRelatedData()
//    }

    return Optional.empty();

  }


}
