package org.gbif.validation.evaluator;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ResourceConstitutionEvaluationChain} is used to build and store the sequence of evaluation that will be performed
 * to evaluate the constitution on the {@link DataFile}.
 *
 * The constitution of a {@link DataFile} represents how the file is composed/represented (e.g. by the meta.xml file) and
 * also how it can be transformed into a {@link DwcDataFile}.
 * An {@link ResourceConstitutionEvaluationChain} is specific to each {@link DataFile} and they should NOT be reused.
 */
@NotThreadSafe
public class ResourceConstitutionEvaluationChain {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceConstitutionEvaluationChain.class);

  private final DataFile dataFile;
  private final List<ResourceStructureEvaluator> resourceStructureEvaluators;
  private final DwcDataFileSupplier dwcDataFileSupplier;
  private final List<DwcDataFileEvaluator> dwcDataFileEvaluators;

  /**
   *
   *
   * @param dataFile
   * @param resourceStructureEvaluators
   * @param dwcDataFileSupplier
   * @param dwcDataFileEvaluators
   */
  ResourceConstitutionEvaluationChain(DataFile dataFile, List<ResourceStructureEvaluator> resourceStructureEvaluators,
                                              DwcDataFileSupplier dwcDataFileSupplier, List<DwcDataFileEvaluator> dwcDataFileEvaluators) {
    this.dataFile = dataFile;
    this.resourceStructureEvaluators = resourceStructureEvaluators;
    this.dwcDataFileSupplier = dwcDataFileSupplier;
    this.dwcDataFileEvaluators = dwcDataFileEvaluators;
  }

  /**
   * Runs all {@link ResourceStructureEvaluator} in the evaluation chain.
   * Breaks (stop the evaluation) if a RESOURCE_INTEGRITY result is received.
   */
  public ResourceConstitutionResult run() {
    List<ValidationResultElement> validationResultElements = new ArrayList<>();
    DwcDataFile transformedDataFile = null;
    boolean evaluationStopped = false;

    for (ResourceStructureEvaluator rsEvaluator : resourceStructureEvaluators) {
      if (!accumulateAndContinue(rsEvaluator.evaluate(dataFile).orElse(null), validationResultElements)) {
        evaluationStopped = true;
        break;
      }
    }

    if (!evaluationStopped && dwcDataFileSupplier != null) {
      try {
        transformedDataFile = dwcDataFileSupplier.get();
        for (DwcDataFileEvaluator rsEvaluator : dwcDataFileEvaluators) {
          if (!accumulateAndContinue(rsEvaluator.evaluate(transformedDataFile).orElse(null), validationResultElements)) {
            evaluationStopped = true;
            break;
          }
        }
      } catch (UnsupportedDataFileException ex) {
        //TODO maybe it would be better to report that as an errorCode and errorMessage?
        validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                EvaluationType.UNHANDLED_ERROR, ex.getMessage()));
        evaluationStopped = true;
      } catch (IOException ex) {
        LOG.error("IOException while transforming dataFile", ex);
        validationResultElements.add(ValidationResultElement.onException(dataFile.getSourceFileName(),
                EvaluationType.UNHANDLED_ERROR, null));
        evaluationStopped = true;
      }
    }
    return new ResourceConstitutionResult(evaluationStopped, transformedDataFile, validationResultElements);
  }

  /**
   *
   * @param result
   * @param accumulator
   * @return should the evaluation continue?
   */
  private static boolean accumulateAndContinue(List<ValidationResultElement> result,
                                    List<ValidationResultElement> accumulator) {
    if(result == null){
      return true;
    }
    accumulator.addAll(result);
    return !containsResourceIntegrity(result);
  }

  /**
   *
   * @param validationResultElements
   * @return Does validationResultElements contains a EvaluationCategory.RESOURCE_INTEGRITY
   */
  private static boolean containsResourceIntegrity(List<ValidationResultElement> validationResultElements) {
    if (validationResultElements == null){
      return false;
    }
    return validationResultElements.stream()
            .filter(vre -> vre.contains(EvaluationCategory.RESOURCE_INTEGRITY))
            .findAny().isPresent();
  }


  /**
   * Object holder to represent ResourceConstitution validation results.
   */
  public static class ResourceConstitutionResult {
    private final boolean evaluationStopped;
    private final DwcDataFile transformedDataFile;
    private final List<ValidationResultElement> results;

    private ResourceConstitutionResult(boolean evaluationStopped, DwcDataFile transformedDataFile, List<ValidationResultElement> results) {
      this.evaluationStopped = evaluationStopped;
      this.transformedDataFile = transformedDataFile;
      this.results = results;
    }

    public boolean isEvaluationStopped() {
      return evaluationStopped;
    }

    public DwcDataFile getTransformedDataFile() {
      return transformedDataFile;
    }

    public List<ValidationResultElement> getResults() {
      return results;
    }
  }
}
