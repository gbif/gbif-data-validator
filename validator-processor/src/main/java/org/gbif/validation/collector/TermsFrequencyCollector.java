package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * Simple collector for terms frequency based on raw data.
 */
public class TermsFrequencyCollector implements RecordMetricsCollector {

  private Term[] columnHeaders;
  private final Map<Term, Long> termFrequencyCounter;

  /**
   *
   * @param columnHeaders
   * @param useConcurrentMap if this {@link ResultsCollector} will be used in a concurrent context
   */
  public TermsFrequencyCollector(Term[] columnHeaders, boolean useConcurrentMap) {
    Validate.notNull(columnHeaders, "columnHeaders must not be null");
    this.columnHeaders = columnHeaders;

    if(useConcurrentMap) {
      termFrequencyCounter = new ConcurrentHashMap<>(columnHeaders.length);
    }
    else{
      termFrequencyCounter = new LinkedHashMap<>(columnHeaders.length);
    }
    for (Term term : columnHeaders) {
      termFrequencyCounter.put(term, 0l);
    }
  }

  @Override
  public void collect(String[] recordData) {
    for (int i = 0; i < recordData.length; i++) {
      if (StringUtils.isNotBlank(recordData[i])) {
        //just in case we have a line larger than the number of column
        if (i < columnHeaders.length) {
          termFrequencyCounter.compute(columnHeaders[i], (k, v) -> ++v);
        }
      }
    }
  }

  @Override
  public Map<Term, Long> getTermFrequency() {
    return termFrequencyCounter;
  }
}
