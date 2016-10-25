package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordMetricsCollector;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 *
 */
public class SimpleRecordMetricsCollector implements RecordMetricsCollector {

  private Term[] columnHeaders;
  private final Map<Term, Long> termFrequencyCounter;

  public SimpleRecordMetricsCollector (Term[] columnHeaders) {
    this.columnHeaders = columnHeaders;
    termFrequencyCounter = new LinkedHashMap<>(columnHeaders.length);
    for(Term term : columnHeaders) {
      termFrequencyCounter.put(term, 0l);
    }
  }

  @Override
  public void collect(String[] recordData) {
    for(int i=0;i<recordData.length;i++){
      if(StringUtils.isNotBlank(recordData[i])) {
        //just in case we have a line larger than the number of column
        if(i < columnHeaders.length) {
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
