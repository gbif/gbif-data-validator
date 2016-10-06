package org.gbif.occurrence.validation.api.model;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of an evaluation at the record level.
 */
public class RecordEvaluationResult {

  private final String recordId;
  private final List<EvaluationResultDetails> details;

  public RecordEvaluationResult(String recordId, List<EvaluationResultDetails> details){
    this.recordId = recordId;
    this.details = details;
  }

  public  List<EvaluationResultDetails> getDetails(){
    return details;
  }

  public static class Builder {
    public String id;
    public List<EvaluationResultDetails> details;

    public Builder withIdentifier(String id){
      this.id = id;
      return this;
    }

    public Builder addInterpretationDetail(OccurrenceIssue issueFlag, Map<Term, String> relatedData) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new RecordInterpretationResultDetails(issueFlag, relatedData));
      return this;
    }

    public Builder addDescription(EvaluationType evaluationType, String description) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new DescriptionEvaluationResultDetails(evaluationType, description));
      return this;
    }

    public RecordEvaluationResult build(){
      return new RecordEvaluationResult(id, details);
    }
  }

  /**
   * Contains details of a RecordInterpretationResult.
   */
  public static class RecordInterpretationResultDetails implements EvaluationResultDetails {
    private final OccurrenceIssue issueFlag;
    private final Map<Term, String> relatedData;

    public RecordInterpretationResultDetails(OccurrenceIssue issueFlag, Map<Term, String> relatedData) {
      this.issueFlag = issueFlag;
      this.relatedData = relatedData;
    }

    public OccurrenceIssue getIssueFlag() {
      return issueFlag;
    }

    public Map<Term, String> getRelatedData() {
      return relatedData;
    }


    @Override
    public EvaluationDetailType getEvaluationDetailType() {
      return issueFlag;
    }
  }


  public static class DescriptionEvaluationResultDetails implements EvaluationResultDetails {
    private final EvaluationType detailType;
    private final String description;

    public DescriptionEvaluationResultDetails(EvaluationType detailType, String description){
      this.detailType = detailType;
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    @Override
    public EvaluationDetailType getEvaluationDetailType() {
      return detailType;
    }
  }

}
