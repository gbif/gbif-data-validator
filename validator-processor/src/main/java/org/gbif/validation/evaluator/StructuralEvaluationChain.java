package org.gbif.validation.evaluator;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The {@link StructuralEvaluationChain} is used to build and store the sequence of evaluation that will be performed.
 * An {@link StructuralEvaluationChain} is specific to each {@link DataFile} and they should NOT be reused.
 */
public class StructuralEvaluationChain {

  private final DataFile dataFile;
  private final List<ResourceStructureEvaluator> resourceStructureEvaluators;

  private boolean evaluationStopped = false;

  /**
   * Use {@link Builder}.
   * @param dataFile
   * @param resourceStructureEvaluators
   */
  private StructuralEvaluationChain(DataFile dataFile, List<ResourceStructureEvaluator> resourceStructureEvaluators) {
    this.dataFile = dataFile;
    this.resourceStructureEvaluators = resourceStructureEvaluators;
  }

  /**
   * Builder class allowing to build an instance of {@link StructuralEvaluationChain}.
   */
  public static class Builder {
    private final DataFile dataFile;
    private final List<ResourceStructureEvaluator> resourceStructureEvaluators = new ArrayList<>();

    /**
     *
     * @param dataFile dataFile received for validation
     * @param factory
     * @return
     */
    public static Builder using(DataFile dataFile, EvaluatorFactory factory) {
      return new Builder(dataFile, factory);
    }

    private Builder(DataFile dataFile, EvaluatorFactory factory) {
      this.dataFile = dataFile;
      resourceStructureEvaluators.add(factory.createResourceStructureEvaluator(dataFile.getFileFormat()));
    }

    public StructuralEvaluationChain build() {
      return new StructuralEvaluationChain(dataFile, resourceStructureEvaluators);
    }
  }

  /**
   * Runs all {@link ResourceStructureEvaluator} in the evaluation chain.
   * Breaks (stop the evaluation) if a RESOURCE_INTEGRITY result is received.
   */
  public Optional<List<ValidationResultElement>> runResourceStructureEvaluator() {
    List<ValidationResultElement> validationResultElements = new ArrayList<>();

    for(ResourceStructureEvaluator rsEvaluator: resourceStructureEvaluators) {
      Optional<List<ValidationResultElement>> vrel = rsEvaluator.evaluate(dataFile);
      vrel.ifPresent(validationResultElements::addAll);
      if(containsResourceIntegrity(vrel.orElse(null))) {
        evaluationStopped = true;
        break;
      }
    }

    return validationResultElements.isEmpty() ? Optional.empty() : Optional.of(validationResultElements);
  }

  private static boolean containsResourceIntegrity(List<ValidationResultElement> validationResultElements) {
    if (validationResultElements != null){
      return false;
    }
    return validationResultElements.stream()
            .filter(vre -> vre.contains(EvaluationCategory.RESOURCE_INTEGRITY))
            .findAny().isPresent();
  }

  /**
   * Checks if the evaluation chain reached the end or it was stopped before.
   * @return
   */
  public boolean evaluationStopped() {
    return evaluationStopped;
  }
}
