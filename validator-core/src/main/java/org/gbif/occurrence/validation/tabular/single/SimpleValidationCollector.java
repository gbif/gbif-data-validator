package org.gbif.occurrence.validation.tabular.single;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.EvaluationResult;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Basic implementation of a {@link ResultsCollector}.
 */
@NotThreadSafe
public class SimpleValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private static final int MAX_SAMPLE_SIZE = 10;

  //collect counts
  private Map<EvaluationType, Map<EvaluationDetailType, Long>> issueCounter = new HashMap<>();
  private Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> issueSampling = new HashMap<>();

  private long recordCount;

  @Override
  public void accumulate(EvaluationResult result) {
    issueSampling.putIfAbsent(result.getEvaluationType(), new HashMap<>());
    issueCounter.putIfAbsent(result.getEvaluationType(), new HashMap<>());

    switch (result.getEvaluationType()) {
      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
        break;
      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretionBasedEvaluationResult)result);
        break;
    }
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> getSamples() {
    return issueSampling;
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, Long>> getAggregatedCounts() {
    return issueCounter;
  }

  //TODO C.G. not DRY
  private void accumulate(RecordStructureEvaluationResult result) {

    Map<EvaluationDetailType, List<EvaluationResultDetails>> currentSample = issueSampling.get(result.getEvaluationType());
    for(EvaluationResultDetails detail : result.getDetails()){
      currentSample.putIfAbsent(detail.getEvaluationDetailType(), new ArrayList<>());
      if(currentSample.get(detail.getEvaluationDetailType()).size() < MAX_SAMPLE_SIZE){
        currentSample.get(detail.getEvaluationDetailType()).add(detail);
      }
      issueCounter.get(result.getEvaluationType()).compute(detail.getEvaluationDetailType(), (k,v) -> (v == null) ? 1 : ++v);
    }
  }

  //TODO C.G. not DRY
  private void accumulate(RecordInterpretionBasedEvaluationResult result) {

    Map<EvaluationDetailType, List<EvaluationResultDetails>> currentSample = issueSampling.get(result.getEvaluationType());
    for(EvaluationResultDetails detail : result.getDetails()){
      currentSample.putIfAbsent(detail.getEvaluationDetailType(), new ArrayList<>());
      if(currentSample.get(detail.getEvaluationDetailType()).size() < MAX_SAMPLE_SIZE){
        currentSample.get(detail.getEvaluationDetailType()).add(detail);
      }
      issueCounter.get(result.getEvaluationType()).compute(detail.getEvaluationDetailType(), (k,v) -> (v == null) ? 1 : ++v);
    }

    recordCount += 1;
  }


  @Override
  public String toString() {
    return "Record count: " + recordCount;// + " Issues: " + issuesCounter.toString();
  }

}
