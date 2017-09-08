package org.gbif.validation.evaluator;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationIssue;
import org.gbif.validation.api.result.ValidationIssues;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.api.vocabulary.DwcFileType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Ensure all the components we expect from {@link DwcDataFile} are available and meet the expected
 * prerequisites.
 */
class PrerequisiteEvaluator implements DwcDataFileEvaluator {

  @Override
  public Optional<List<ValidationResultElement>> evaluate(DwcDataFile dwcDataFile) {

    if (dwcDataFile.getCore() == null || dwcDataFile.getCore().getRowTypeKey() == null ||
            dwcDataFile.getCore().getRowTypeKey().getRowType() == null) {
      return Optional.of(
              generateValidationResultElement(dwcDataFile.getDataFile().getSourceFileName(),
                      EvaluationType.CORE_ROWTYPE_UNDETERMINED));
    }

    if (!dwcDataFile.getCore().getRecordIdentifier().isPresent()) {
      return Optional.of(
              generateValidationResultElement(dwcDataFile.getDataFile().getSourceFileName(),
                      EvaluationType.RECORD_IDENTIFIER_NOT_FOUND));
    }

    return Optional.empty();
  }

  /**
   * Generate a {@code List<ValidationResultElement>} for a single {@link EvaluationType} one the core.
   *
   * @param filename
   * @param evaluationType
   *
   * @return
   */
  private static List<ValidationResultElement> generateValidationResultElement(String filename, EvaluationType evaluationType) {
    List<ValidationResultElement> validationResultElements = new ArrayList<>();
    List<ValidationIssue> validationIssues = new ArrayList<>();
    validationIssues.add(ValidationIssues.withEvaluationTypeOnly(evaluationType));
    validationResultElements.add(
            new ValidationResultElement(
                    filename, null, DwcFileType.CORE,
                    null, validationIssues));
    return validationResultElements;
  }
}
