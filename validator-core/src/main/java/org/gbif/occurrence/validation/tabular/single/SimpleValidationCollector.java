package org.gbif.occurrence.validation.tabular.single;

import org.gbif.api.vocabulary.EvaluationDetailType;
import org.gbif.api.vocabulary.EvaluationType;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.EvaluationResult;
import org.gbif.occurrence.validation.model.EvaluationResultDetails;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Basic implementation of a {@link ResultsCollector}.
 */
@NotThreadSafe
public class SimpleValidationCollector implements ResultsCollector {

  private final int maxNumberOfSample;

  private final Map<EvaluationType, Map<EvaluationDetailType, Long>> issueCounter;
  private final Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> issueSampling;

  private long recordCount;

  public SimpleValidationCollector(Integer maxNumberOfSample) {
    this.maxNumberOfSample = (maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE);

    issueCounter = new HashMap<>(EvaluationType.values().length);
    issueSampling = new HashMap<>(EvaluationType.values().length);
  }

  @Override
  public void accumulate(EvaluationResult result) {
    issueSampling.putIfAbsent(result.getEvaluationType(), new HashMap<>());
    issueCounter.putIfAbsent(result.getEvaluationType(), new HashMap<>());

    collectData(result.getDetails(), issueCounter.get(result.getEvaluationType()),
            issueSampling.get(result.getEvaluationType()));

//    switch (result.getEvaluationType()) {
//      case STRUCTURE_EVALUATION: accumulate((RecordStructureEvaluationResult)result);
//        break;
//      case INTERPRETATION_BASED_EVALUATION: accumulate((RecordInterpretionBasedEvaluationResult)result);
//        break;
//    }
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, List<EvaluationResultDetails>>> getSamples() {
    return issueSampling;
  }

  @Override
  public Map<EvaluationType, Map<EvaluationDetailType, Long>> getAggregatedCounts() {
    return issueCounter;
  }


  /**
   * Function used to collect data from a list of {@link EvaluationResultDetails}.
   *
   * @param details
   * @param counter
   * @param samples
   */
  private void collectData(List<EvaluationResultDetails> details,
                           Map<EvaluationDetailType, Long> counter,
                           Map<EvaluationDetailType, List<EvaluationResultDetails>> samples){
    details.forEach(
            detail -> {
              counter.compute(detail.getEvaluationDetailType(), (k, v) -> (v == null) ? 1 : ++v);

              samples.putIfAbsent(detail.getEvaluationDetailType(), new ArrayList<>());
              samples.compute(detail.getEvaluationDetailType(), (type, queue) -> {
                if(queue.size() < maxNumberOfSample){
                  samples.get(type).add(detail);
                }
                return queue;
              });
            }
    );
  }


  @Override
  public String toString() {
    return "Record count: " + recordCount;// + " Issues: " + issuesCounter.toString();
  }

}
