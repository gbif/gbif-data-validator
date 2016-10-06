package org.gbif.occurrence.validation.tabular.single;

import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.api.model.EvaluationResultDetails;
import org.gbif.occurrence.validation.api.model.EvaluationType;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

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

  private final Map<EvaluationType, Long> issueCounter;
  private final Map<EvaluationType, List<EvaluationResultDetails>> issueSampling;

  public SimpleValidationCollector(Integer maxNumberOfSample) {
    this.maxNumberOfSample = maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE;

    issueCounter = new HashMap<>();
    issueSampling = new HashMap<>();
  }

  @Override
  public void accumulate(RecordEvaluationResult result) {

    if(result.getDetails() == null){
      return;
    }

    result.getDetails().forEach(
            detail -> {
              issueCounter.compute(detail.getEvaluationDetailType(), (k, v) -> (v == null) ? 1 : ++v);

              issueSampling.putIfAbsent(detail.getEvaluationDetailType(), new ArrayList<>());
              issueSampling.compute(detail.getEvaluationDetailType(), (type, queue) -> {
                if(queue.size() < maxNumberOfSample){
                  issueSampling.get(type).add(detail);
                }
                return queue;
              });
            }
    );
  }

  @Override
  public Map<EvaluationType, List<EvaluationResultDetails>> getSamples() {
    return issueSampling;
  }

  @Override
  public Map<EvaluationType, Long> getAggregatedCounts() {
    return issueCounter;
  }


}
