package org.gbif.occurrence.validation.api.model;

import org.gbif.api.vocabulary.EvaluationDetailType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Contains the result of a DataFile validation
 */
public class DataFileValidationResult {

  public enum Status {OK, FAILED};

  private Status status;
  private boolean indexeable;

  //only used in case of general error with the input file
  private String error;

  private List<DateFileValidationElement> issues = new ArrayList<>();


  public DataFileValidationResult(Map<EvaluationDetailType, Long> issueCounter,
                                  Map<EvaluationDetailType, List<EvaluationResultDetails>> issueSampling) {
    issueCounter.forEach(
            (k, v) ->
                    issues.add(new DateFileValidationElement(k, v, issueSampling.get(k)))
    );
  }


  public List<DateFileValidationElement> getIssues(){
    return issues;
  }


  private static class DateFileValidationElement {

    private EvaluationDetailType evaluationSubType;
    private long count;
    private List<EvaluationResultDetails> sample;

    public DateFileValidationElement(EvaluationDetailType evaluationSubType, long count, List<EvaluationResultDetails> sample){
      this.evaluationSubType = evaluationSubType;
      this.count = count;
      this.sample = sample;
    }

    public EvaluationDetailType getEvaluationSubType() {
      return evaluationSubType;
    }

    public long getCount() {
      return count;
    }

    public List<EvaluationResultDetails> getSample() {
      return sample;
    }
  }

}
