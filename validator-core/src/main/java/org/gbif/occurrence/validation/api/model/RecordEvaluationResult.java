package org.gbif.occurrence.validation.api.model;

import org.gbif.dwc.terms.Term;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of an evaluation at the record level.
 * Immutable once built.
 */
public class RecordEvaluationResult implements Serializable {

  //currently stored inside the details
  private final String recordId;
  private final List<EvaluationResultDetails> details;

  public RecordEvaluationResult(String recordId, List<EvaluationResultDetails> details){
    this.recordId = recordId;
    this.details = details;
  }

  public List<EvaluationResultDetails> getDetails(){
    return details;
  }

  /**
   * Builder class to build a RecordEvaluationResult instance.
   */
  public static class Builder {
    private Long lineNumber;
    private String recordId;
    private List<EvaluationResultDetails> details;

    public Builder withLineNumber(long lineNumber){
      this.lineNumber = lineNumber;
      return this;
    }

    public Builder withRecordId(String recordId){
      this.recordId = recordId;
      return this;
    }

    public Builder addInterpretationDetail(EvaluationType issueFlag, Map<Term, String> relatedData) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new RecordInterpretationResultDetails(lineNumber, recordId, issueFlag, relatedData));
      return this;
    }

    public Builder addDescription(EvaluationType evaluationType, String description) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new DescriptionEvaluationResultDetails(lineNumber, recordId, evaluationType, description));
      return this;
    }

    public RecordEvaluationResult build(){
      return new RecordEvaluationResult(recordId, details);
    }
  }


  /**
   *
   */
  public static class DescriptionEvaluationResultDetails implements EvaluationResultDetails {
    protected Long lineNumber;
    protected String recordId;
    protected final EvaluationType evaluationType;
    protected final String description;

    DescriptionEvaluationResultDetails(Long lineNumber, String recordId, EvaluationType evaluationType,
                                       String description){
      this.evaluationType = evaluationType;
      this.description = description;
    }

    public Long getLineNumber(){
      return lineNumber;
    }

    public String getRecordId() {
      return recordId;
    }

    public String getDescription() {
      return description;
    }

    @Override
    public EvaluationType getEvaluationType() {
      return evaluationType;
    }
  }

  /**
   * Contains details of a RecordInterpretationResult.
   */
  public static class RecordInterpretationResultDetails extends DescriptionEvaluationResultDetails {

    private final Map<Term, String> relatedData;

    RecordInterpretationResultDetails(Long lineNumber, String recordId, EvaluationType issueFlag,
                                             Map<Term, String> relatedData) {
      super(lineNumber, recordId, issueFlag, null);
      this.relatedData = relatedData;
    }

    public Map<Term, String> getRelatedData() {
      return relatedData;
    }
  }
  
}
