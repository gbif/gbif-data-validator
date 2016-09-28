package org.gbif.occurrence.validation.model;

import org.gbif.api.vocabulary.EvaluationType;

import java.util.List;

/**
 * Class representing the abstraction of an evaluation result.
 */
public abstract class EvaluationResult {

  private final String id;
  private final EvaluationType evaluationType;

  public EvaluationResult(String id, EvaluationType evaluationType){
    this.id = id;
    this.evaluationType = evaluationType;
  }

  public String getId() {
    return id;
  }

  public EvaluationType getEvaluationType() {
    return evaluationType;
  }

  public abstract List<EvaluationResultDetails> getDetails();

}
