package org.gbif.occurrence.validation.model;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 *
 * Result of an Interpretation Based Evaluation
 */
public class RecordInterpretationResult extends EvaluationResult {

  private final List<EvaluationResultDetails> details;

  /**
   * Private constructor, use builder.
   *
   * @param recordId
   * @param details
   */
  private RecordInterpretationResult(String recordId,
                                     List<EvaluationResultDetails> details){
    super(recordId, EvaluationType.INTERPRETATION_BASED_EVALUATION);
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

    public Builder addDetail(OccurrenceIssue issueFlag, Map<Term, String> relatedData) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new Details(issueFlag, relatedData));
      return this;
    }

    public RecordInterpretationResult build(){
      return new RecordInterpretationResult(id, details);
    }
  }

  /**
   * Contains details of a RecordInterpretationResult.
   */
  public static class Details implements EvaluationResultDetails {
    private final OccurrenceIssue issueFlag;
    private final Map<Term, String> relatedData;

    public Details(OccurrenceIssue issueFlag, Map<Term, String> relatedData) {
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
}
