package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.api.result.ValidationResultDetails;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
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
    innerImpl = useConcurrentMap ? new RecordEvaluationResultCollectorConcurrent() :
            new RecordEvaluationResultCollectorSingleThread();
  }

  @Override
  public void collect(RecordEvaluationResult result) {
    if (result != null && result.getDetails() != null) {
      result.getDetails().forEach(detail -> {
        innerImpl.countAndPrepare(detail.getEvaluationType());
        innerImpl.computeSampling(detail.getEvaluationType(), (type, currSample) -> {
          if (currSample.size() < maxNumberOfSample) {
            String key = detail.computeInputValuesKey();
            if (!currSample.containsKey(key)) {
              currSample.put(key, toValidationResultDetails(result, detail));
              if(result.getLineNumber() != null && result.getVerbatimData() != null) {
                innerImpl.putVerbatimRecord(result.getLineNumber(), result.getVerbatimData());
              }
            } else {
              innerImpl.putNonDistinct(detail.getEvaluationType(), toValidationResultDetails(result, detail));
            }
          }
          return currSample;
        });
      });
    }
  }

  /**
   * Internal interface that defined the behavior of an internal RecordEvaluationResultCollector.
   */
  private interface InnerRecordEvaluationResultCollector extends Serializable {
    void countAndPrepare(EvaluationType type);
    void computeSampling(EvaluationType type, BiFunction<EvaluationType, Map<String, ValidationResultDetails>,
            Map<String, ValidationResultDetails>> samplingFunction);
    void putNonDistinct(EvaluationType type, ValidationResultDetails validationResultDetails);
    void putVerbatimRecord(Long recordNumber, Map<Term, String> verbatimRecord);

    Map<EvaluationType, Collection<ValidationResultDetails>> getNonDistinct();
    Map<EvaluationType, Long> getAggregatedCounts();
    Map<EvaluationType, List<ValidationResultDetails>> getSamples();
    Map<Long, Map<Term, String>> getFullRecordSample();
  }

  /**
   * InnerRecordEvaluationResultCollector implementation with support for single-thread access.
   */
  private static class RecordEvaluationResultCollectorSingleThread implements InnerRecordEvaluationResultCollector {
    private final Map<EvaluationType, Long> issueCounter;
    private final Map<EvaluationType, Map<String, ValidationResultDetails>> issueSampling;
    private final Map<EvaluationType, Collection<ValidationResultDetails>> nonDistinctSample;
    private final Map<Long, Map<Term, String>> fullRecordSample;

    RecordEvaluationResultCollectorSingleThread() {
      issueCounter = new EnumMap<>(EvaluationType.class);
      issueSampling = new EnumMap<>(EvaluationType.class);
      nonDistinctSample = new EnumMap<>(EvaluationType.class);
      fullRecordSample = new HashMap<>();
    }

    @Override
    public void countAndPrepare(EvaluationType type) {
      issueCounter.compute(type, (k, v) -> (v == null) ? 1 : ++v);
      issueSampling.putIfAbsent(type, new HashMap<>());
    }

    @Override
    public void computeSampling(EvaluationType type, BiFunction<EvaluationType, Map<String, ValidationResultDetails>,
            Map<String, ValidationResultDetails>> samplingFunction) {
      issueSampling.compute(type, samplingFunction);
    }

    @Override
    public void putNonDistinct(EvaluationType type, ValidationResultDetails validationResultDetails) {
      nonDistinctSample.putIfAbsent(type, new ArrayList<>());
      nonDistinctSample.compute(type, (k, l) -> {
        l.add(validationResultDetails);
        return l;
      });
    }

    @Override
    public Map<EvaluationType, Collection<ValidationResultDetails>> getNonDistinct() {
      return nonDistinctSample;
    }

    @Override
    public Map<EvaluationType, List<ValidationResultDetails>> getSamples() {
      return issueSampling.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
              rec ->  new ArrayList<>(rec.getValue().values())));
    }

    @Override
    public Map<Long, Map<Term, String>> getFullRecordSample() {
      return fullRecordSample;
    }

    @Override
    public Map<EvaluationType, Long> getAggregatedCounts() {
      return issueCounter.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
              Map.Entry::getValue));
    }

    /**
     *
     * @param recordNumber
     * @param verbatimRecord
     */
    public void putVerbatimRecord(Long recordNumber, Map<Term, String> verbatimRecord) {
      fullRecordSample.put(recordNumber, verbatimRecord);
    }
  }

  /**
   * InnerRecordEvaluationResultCollector implementation with support for concurrent access.
   */
  private static class RecordEvaluationResultCollectorConcurrent implements InnerRecordEvaluationResultCollector {
    private final Map<EvaluationType, LongAdder> issueCounter;
    private final Map<EvaluationType, Map<String, ValidationResultDetails>> issueSampling;
    private final Map<EvaluationType, Collection<ValidationResultDetails>> nonDistinctSample;
    private final Map<Long, Map<Term, String>> fullRecordSample;

    RecordEvaluationResultCollectorConcurrent() {
      issueCounter = new ConcurrentHashMap<>(EvaluationType.values().length);
      issueSampling = new ConcurrentHashMap<>(EvaluationType.values().length);
      nonDistinctSample = new ConcurrentHashMap<>(EvaluationType.values().length);
      fullRecordSample = new ConcurrentHashMap<>();
    }

    @Override
    public void countAndPrepare(EvaluationType type) {
      issueCounter.computeIfAbsent(type, k -> new LongAdder()).increment();
      issueSampling.putIfAbsent(type, new ConcurrentHashMap<>());
    }

    @Override
    public void computeSampling(EvaluationType type, BiFunction<EvaluationType, Map<String,ValidationResultDetails>,
            Map<String,ValidationResultDetails>> samplingFunction) {
      issueSampling.compute(type, samplingFunction);
    }

    @Override
    public void putNonDistinct(EvaluationType type, ValidationResultDetails validationResultDetails) {
      nonDistinctSample.putIfAbsent(type, new ConcurrentLinkedQueue<>());
      nonDistinctSample.compute(type, (k, l) -> {
        l.add(validationResultDetails);
        return l;
      });
    }

    @Override
    public void putVerbatimRecord(Long recordNumber, Map<Term, String> verbatimRecord) {
      fullRecordSample.put(recordNumber, verbatimRecord);
    }

    @Override
    public Map<EvaluationType, Collection<ValidationResultDetails>> getNonDistinct() {
      return nonDistinctSample;
    }

    /**
     * @return a copy of the inter aggregated counts.
     */
    @Override
    public Map<EvaluationType, Long> getAggregatedCounts() {
      return issueCounter.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
              rec -> rec.getValue().longValue()));
    }

    /**
     *
     * @return a copy of the internal evaluation samples.
     */
    @Override
    public Map<EvaluationType, List<ValidationResultDetails>> getSamples() {
      return issueSampling.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
              rec ->  new ArrayList<>(rec.getValue().values())));
    }

    @Override
    public Map<Long, Map<Term, String>> getFullRecordSample() {
      return fullRecordSample;
    }
  }

  private static ValidationResultDetails toValidationResultDetails(RecordEvaluationResult result, RecordEvaluationResultDetails detail) {
    return new ValidationResultDetails(result.getLineNumber(),
            result.getRecordId(), detail.getExpected(), detail.getFound(), detail.getRelatedData());
  }

  public Map<EvaluationType, List<ValidationResultDetails>> getSamples() {
    Map<EvaluationType, List<ValidationResultDetails>> samplesCopy = innerImpl.getSamples();
    Map<EvaluationType, Collection<ValidationResultDetails>> nonDistinctSample = innerImpl.getNonDistinct();
    samplesCopy.forEach( (et, sample) -> {
      if(sample.size() < maxNumberOfSample && nonDistinctSample.get(et) != null){
        for(ValidationResultDetails nonDistinctElement : nonDistinctSample.get(et)){
          sample.add(nonDistinctElement);
          if(maxNumberOfSample - sample.size() == 0){
            break;
          }
        }
      }
    });
    return samplesCopy;
  }

  public Map<Long, Map<Term, String>> getFullRecordSamples(){
    return innerImpl.getFullRecordSample();
  }

  public Map<EvaluationType, Long> getAggregatedCounts() {
    return innerImpl.getAggregatedCounts();
  }

}
