package org.gbif.validation.evaluator;

import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to evaluate the structure of a resource.
 * For DarwinCore Archive that includes XML files schema validation.
 */
public class DwcaResourceStructureEvaluator implements ResourceStructureEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaResourceStructureEvaluator.class);

  public Optional<ValidationResultElement> evaluate(Path dwcFolder, String sourceFilename) {
    try {
      ArchiveFactory.openArchive(dwcFolder.toFile());
    } catch (UnsupportedArchiveException uaEx) {
      return Optional.of(ValidationResultBuilders.DefaultValidationResultElementBuilder
              .of(sourceFilename).addExceptionResultDetails(EvaluationType.DWCA_UNREADABLE,
                      uaEx.getMessage()).build());
    } catch (IOException e) {
      LOG.error("Can't evaluateDwca", e);
    }
    return Optional.empty();
  }
}
