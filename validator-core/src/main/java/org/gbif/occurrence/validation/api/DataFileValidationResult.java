package org.gbif.occurrence.validation.api;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataFileValidationResult {


  private Map<EvaluationType, List<DateFileValidationElement>> results = new HashMap<>();

  public DataFileValidationResult(Map<EvaluationType, Map<EvaluationDetailType, Long>> issueCounter,
                                  Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> issueSampling) {

    issueCounter.forEach(
            (k,v) -> {
              results.putIfAbsent(k, new ArrayList<>());
              v.forEach(
                    (k1, v1) -> results.get(k).add(new DateFileValidationElement(k1, v1, issueSampling.get(k).get(k1)))
              );
            }
    );
  }


  public Map<EvaluationType, List<DateFileValidationElement>> getResults(){
    return results;
  }

//  @Override
//  public String toString() {
//
//    String toString = "DataFileValidationResult{" +
//            "issues=" + issues;
//
//    //temporary code, will be replaced by json serialisation
//    if(recordStructureIssue !=null && !recordStructureIssue.isEmpty()){
//      toString += ", recordStructureIssue{[";
//      for(RecordStructureEvaluationResult result : recordStructureIssue){
//        toString += "{lineNumber:"+result.getId() + ", " + result.getDetails()+"}, ";
//      }
//
//      toString = StringUtils.removeEnd(toString, ",");
//      toString += "]}";
//    }
//
//    toString += '}';
//
//    return toString;
//  }

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
