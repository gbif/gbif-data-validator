package org.gbif.validation.collector;

import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.result.EvaluationResultDetails;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Basic implementation of a {@link ResultsCollector}.
 */
public class RecordEvaluationResultCollector implements ResultsCollector, Serializable {

  public static final int DEFAULT_MAX_NUMBER_OF_SAMPLE = 10;

  private final int maxNumberOfSample;
  private final InnerRecordEvaluationResultCollector innerImpl;

  /**
   *
   * @param maxNumberOfSample
   * @param useConcurrentMap if this {@link ResultsCollector} will be used in a concurrent context
   */
  public RecordEvaluationResultCollector(Integer maxNumberOfSample, boolean useConcurrentMap) {
    this.maxNumberOfSample = maxNumberOfSample != null ? maxNumberOfSample : DEFAULT_MAX_NUMBER_OF_SAMPLE;

    if(useConcurrentMap) {
      innerImpl = new RecordEvaluationResultCollectorConcurrent(-1);
    }
    else {
      innerImpl = new RecordEvaluationResultCollectorSingleThread(-1);
    }
  }

  private interface InnerRecordEvaluationResultCollector extends Serializable {
    void countAndPrepare(EvaluationType type);
    void computeSampling(EvaluationType type, BiFunction<EvaluationType, Collection<SampledEvaluationResultDetails>,
            Collection<SampledEvaluationResultDetails>> samplingFunction);
    Map<EvaluationType, Long> getAggregatedCounts();
    Map<EvaluationType, List<SampledEvaluationResultDetails>> getSamples();
  }

  private static class RecordEvaluationResultCollectorSingleThread implements InnerRecordEvaluationResultCollector {
    private final Map<EvaluationType, Long> issueCounter;
    private final Map<EvaluationType, Collection<SampledEvaluationResultDetails>> issueSampling;

    RecordEvaluationResultCollectorSingleThread(Integer maxNumberOfSample) {
      issueCounter = new EnumMap<>(EvaluationType.class);
      issueSampling = new EnumMap<>(EvaluationType.class);
    }

    @Override
    public void countAndPrepare(EvaluationType type) {
      issueCounter.compute(type, (k, v) -> (v == null) ? 1 : ++v);
      issueSampling.putIfAbsent(type, new ArrayList<>());
    }

    @Override
    public void computeSampling(EvaluationType type, BiFunction<EvaluationType, Collection<SampledEvaluationResultDetails>,
            Collection<SampledEvaluationResultDetails>> samplingFunction) {
      issueSampling.compute(type, samplingFunction);
    }

    @Override
    public Map<EvaluationType, List<SampledEvaluationResultDetails>> getSamples() {
      return issueSampling.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, //Key
              rec ->  new ArrayList<>(rec.getValue()))); //Value
    }

    @Override
    public Map<EvaluationType, Long> getAggregatedCounts() {
      return issueCounter.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, //Key
              Map.Entry::getValue)); //Value
    }
  }

  private static class RecordEvaluationResultCollectorConcurrent implements InnerRecordEvaluationResultCollector {
    private final Map<EvaluationType, LongAdder> issueCounter;
    private final Map<EvaluationType, Collection<SampledEvaluationResultDetails>> issueSampling;

    RecordEvaluationResultCollectorConcurrent(Integer maxNumberOfSample) {
      issueCounter = new ConcurrentHashMap<>(EvaluationType.values().length);
      issueSampling = new ConcurrentHashMap<>(EvaluationType.values().length);
    }

    @Override
    public void countAndPrepare(EvaluationType type) {
      issueCounter.computeIfAbsent(type, k -> new LongAdder()).increment();
      issueSampling.putIfAbsent(type, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void computeSampling(EvaluationType type, BiFunction<EvaluationType, Collection<SampledEvaluationResultDetails>,
            Collection<SampledEvaluationResultDetails>> samplingFunction) {
      issueSampling.compute(type, samplingFunction);
    }

    /**
     * @return a copy of the inter aggregated counts.
     */
    @Override
    public Map<EvaluationType, Long> getAggregatedCounts() {
      return issueCounter.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, //Key
              rec -> rec.getValue().longValue())); //Value
    }

    /**
     *
     * @return a copy of the internal evaluation samples.
     */
    @Override
    public Map<EvaluationType, List<SampledEvaluationResultDetails>> getSamples() {
      return issueSampling.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, //Key
              rec ->  new ArrayList<>(rec.getValue()))); //Value
    }
  }


  @Override
  public void collect(RecordEvaluationResult result) {
    //result.
    if (result !=null && result.getDetails() != null) {
      result.getDetails().forEach(detail -> {
        innerImpl.countAndPrepare(detail.getEvaluationType());
        innerImpl.computeSampling(detail.getEvaluationType(), (type, queue) -> {
          if (queue.size() < maxNumberOfSample) {
            queue.add(new SampledEvaluationResultDetails(result.getLineNumber(), result.getRecordId(), detail));
          }
          return queue;
        });
      });
    }
  }


  public Map<EvaluationType, List<SampledEvaluationResultDetails>> getSamples() {
    return innerImpl.getSamples();
  }

  public Map<EvaluationType, Long> getAggregatedCounts() {
    return innerImpl.getAggregatedCounts();
  }

  /**
   * Wrapper class around EvaluationResultDetails to keep the context of the {@link EvaluationResultDetails}
   * when aggregated by issue type.
   */
  static class SampledEvaluationResultDetails {

    private String recordId;
    private Long lineNumber;
    private EvaluationResultDetails details;

    SampledEvaluationResultDetails(Long lineNumber, String recordId, EvaluationResultDetails details){
      this.lineNumber = lineNumber;
      this.recordId = recordId;
      this.details = details;
    }

    public String getRecordId(){
      return recordId;
    }

    public Long getLineNumber(){
      return lineNumber;
    }

    public EvaluationResultDetails getDetails(){
      return details;
    }
  }

}
