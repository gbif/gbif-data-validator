package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

import java.util.Map;

/**
 * Interface defining the collector of metrics on record.
 */
public interface RecordMetricsCollector {

  void collect(String[] recordData);

  Map<Term, Long> getTermFrequency();
}
