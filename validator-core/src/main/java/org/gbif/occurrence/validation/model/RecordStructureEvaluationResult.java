package org.gbif.occurrence.validation.model;


import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of an evaluation on the structure of the data.
 */
public class RecordStructureEvaluationResult extends EvaluationResult {

  private final List<EvaluationResultDetails> details;

  private RecordStructureEvaluationResult(String recordId, List<EvaluationResultDetails> details){
    super(recordId, EvaluationType.STRUCTURE_EVALUATION);
    this.details = details;
  }

  @Override
  public List<EvaluationResultDetails> getDetails() {
    return details;
  }

  public static class Builder {
    public String id;
    public List<EvaluationResultDetails> details;

    public Builder withIdentifier(String id){
      this.id = id;
      return this;
    }

    public Builder addDetail(StructureEvaluationDetailType issueFlag, String relatedData){
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new Details(issueFlag, relatedData));
      return this;
    }

    public RecordStructureEvaluationResult build(){
      return new RecordStructureEvaluationResult(id, details);
    }
  }

  /**
   * Contains details of a RecordInterpretionBasedEvaluationResult.
   */
  public static class Details implements EvaluationResultDetails {
    private final StructureEvaluationDetailType detailType;
    private final String relatedData;

    public Details(StructureEvaluationDetailType detailType, String relatedData){
      this.detailType = detailType;
      this.relatedData = relatedData;
    }

    public String getRelatedData() {
      return relatedData;
    }

    @Override
    public EvaluationDetailType getEvaluationDetailType() {
      return detailType;
    }
  }
}
