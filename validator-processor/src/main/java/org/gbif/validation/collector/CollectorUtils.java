package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Utility functions for collectors.
 */
public class CollectorUtils {

  /**
   * Private constructor.
   */
  private CollectorUtils() {
    //empty constructor
  }
  /**
   * Initializes a Map<Term,Long> using the terms[] for keys and zeroes for all the values.
   */
  public static Map<Term, Long> getZeroTermFrequency(Term[] terms, boolean useConcurrentMap) {
    return getZeroTermFrequency(Arrays.stream(terms), terms.length, useConcurrentMap);
  }

  /**
   * Initializes a Map<Term,Long> using the List<Terms> for keys and zeroes for all the values.
   */
  public static Map<Term, Long> getZeroTermFrequency(List<Term> terms, boolean useConcurrentMap) {
    return getZeroTermFrequency(terms.stream(),terms.size(),useConcurrentMap);
  }

  /**
   * Initializes a Map<Term,Long> using the Stream<Terms> for keys and zeroes for all the values.
   */
  public static Map<Term, Long> getZeroTermFrequency(Stream<Term> terms, int size, boolean useConcurrentMap) {
    Map<Term, Long> termFrequencyCounter = useConcurrentMap ? new ConcurrentHashMap<>(size) :
                                                              new LinkedHashMap<>(size);
    terms.forEach(term  -> termFrequencyCounter.put(term, 0l));
    return termFrequencyCounter;
  }
}
