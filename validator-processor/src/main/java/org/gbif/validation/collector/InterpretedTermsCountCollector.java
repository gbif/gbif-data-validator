package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Validate;

/**
 * Simple collector to count interpreted data from a predefined list of terms.
 *
 */
public class InterpretedTermsCountCollector implements ResultsCollector, Serializable {

  private final List<Term> targetedTerms;

  private final Map<Term, Long> interpretedValueCounter;

  //private static final MAX_DISTINCT_COUNT
  //private final Map<Term, Map<Object, Long>> interpretedValueDistinctCounter;

  /**
   *
   * @param targetedTerms terms from which we should count if there is a value
   * @param useConcurrentMap if this {@link ResultsCollector} will be used in a concurrent context
   */
  public InterpretedTermsCountCollector(List<Term> targetedTerms, boolean useConcurrentMap) {
    Validate.notNull(targetedTerms, "targetedTerms must not be null");

    this.targetedTerms = targetedTerms;

    interpretedValueCounter = useConcurrentMap ? new ConcurrentHashMap<>(targetedTerms.size()):
                                                 new HashMap<>(targetedTerms.size());

    for(Term term : targetedTerms) {
      interpretedValueCounter.put(term, 0l);
    }
  }

  public Map<Term, Long> getInterpretedCounts() {
    return interpretedValueCounter;
  }

  @Override
  public void collect(RecordEvaluationResult result) {
    if(result == null ){
      return;
    }
    Map<Term, Object> terms = result.getInterpretedData();
    if(terms == null ){
      return;
    }

    for(Term term : targetedTerms) {
      interpretedValueCounter.compute(term, (k,v) -> terms.get(term) != null ? ++v : v);
    }
  }
}
