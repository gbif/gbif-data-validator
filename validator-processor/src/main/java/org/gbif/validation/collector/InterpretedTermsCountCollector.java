package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

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
    interpretedValueCounter = CollectorUtils.getZeroTermFrequency(targetedTerms, useConcurrentMap);
  }

  public Map<Term, Long> getInterpretedCounts() {
    return interpretedValueCounter;
  }

  @Override
  public void collect(RecordEvaluationResult result) {
    BiConsumer<Term,Map<Term, Object>> increment = (term, terms) -> interpretedValueCounter.compute(term, (k, v) ->
                                                                    terms.get(term) != null ? ++v : v);
    Optional.ofNullable(result)
      .ifPresent(r -> Optional.ofNullable(r.getInterpretedData())
        .ifPresent(terms -> targetedTerms.stream().forEach(term -> increment.accept(term,terms))));
  }
}
