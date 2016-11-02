package org.gbif.validation.tabular.single;

import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.EvaluationResultDetails;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Basic implementation of a {@link ResultsCollector}.
 */
@NotThreadSafe
public class SimpleValidationCollector implements ResultsCollector, Serializable {

  //TODO provide in config
  public static final int DEFAULT_MAX_NUMBER_OF_SAMPLE = 10;

  private final int maxNumberOfSample;

  private final Map<EvaluationType, Long> issueCounter;
  private final Map<EvaluationType, List<EvaluationResultDetails>> issueSampling;


  public SimpleValidationCollector(Integer maxNumberOfSample) {
    this.maxNumberOfSample = maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE;

    issueCounter = new EnumMap<>(EvaluationType.class);
    issueSampling = new EnumMap<>(EvaluationType.class);
  }

  @Override
  public void collect(RecordEvaluationResult result) {

    if (result.getDetails() != null) {

      result.getDetails().forEach(detail -> {
        issueCounter.compute(detail.getEvaluationType(), (k, v) -> (v == null) ? 1 : ++v);

        issueSampling.putIfAbsent(detail.getEvaluationType(), new ArrayList<>());
        issueSampling.compute(detail.getEvaluationType(), (type, queue) -> {
          if (queue.size() < maxNumberOfSample) {
            issueSampling.get(type).add(detail);
          }
          return queue;
        });
      });
    }
  }

  public Map<EvaluationType, List<EvaluationResultDetails>> getSamples() {
    return issueSampling;
  }

  public Map<EvaluationType, Long> getAggregatedCounts() {
    return issueCounter;
  }

}
