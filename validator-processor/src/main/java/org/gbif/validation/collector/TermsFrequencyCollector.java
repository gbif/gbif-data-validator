package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;

/**
 * Simple collector for terms frequency based on raw data.
 */
public class TermsFrequencyCollector implements RecordMetricsCollector, Serializable {

  private final Term[] columnHeaders;
  private final Map<Term, Long> termFrequencyCounter;

  /**
   *
   * @param terms columnHeaders
   * @param useConcurrentMap if this {@link ResultsCollector} will be used in a concurrent context
   */
  public TermsFrequencyCollector(List<Term> terms, boolean useConcurrentMap) {
    Objects.requireNonNull(terms, "columnHeaders must not be null");
    columnHeaders = terms.toArray(new Term[terms.size()]);
    termFrequencyCounter = CollectorUtils.getZeroTermFrequency(columnHeaders, useConcurrentMap);
  }

  @Override
  public void collect(String[] recordData) {
    IntStream.range(0, Math.min(recordData.length, columnHeaders.length))
      .filter(i -> StringUtils.isNotBlank(recordData[i]))
      .forEach(i -> termFrequencyCounter.compute(columnHeaders[i], (k, v) -> ++v));
  }

  @Override
  public Map<Term, Long> getTermFrequency() {
    return termFrequencyCounter;
  }
}
