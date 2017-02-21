package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.EvaluationType;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;

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
  public static Map<Term, Long> getZeroTermFrequency(Collection<Term> terms, boolean useConcurrentMap) {
    return getZeroTermFrequency(terms.stream(),terms.size(),useConcurrentMap);
  }

  /**
   * Initializes a Map<Term,Long> using the Stream<Terms> for keys and zeroes for all the values.
   * @param terms if the Stream contains null, they will be ignored.
   * @param size
   * @param useConcurrentMap
   */
  public static Map<Term, Long> getZeroTermFrequency(Stream<Term> terms, int size, boolean useConcurrentMap) {
    Map<Term, Long> termFrequencyCounter = useConcurrentMap ? new ConcurrentHashMap<>(size) :
            new LinkedHashMap<>(size);
    terms.filter(t-> t != null).forEach(term -> termFrequencyCounter.put(term, 0l));
    return termFrequencyCounter;
  }

  /**
   * Creates a new EnumMap instance using EvaluationType as key.
   * Optionally, a Map containing data to put initially in the map can be provided.
   *
   * @param initMap
   * @param <V>
   *
   * @return
   */
  public static <V> Map<EvaluationType, V> newEvaluationTypeEnumMap(@Nullable Map<EvaluationType, V> initMap) {
    return newEnumMapInit(EvaluationType.class, Optional.ofNullable(initMap));
  }

  /**
   * Creates a new EnumMap instance using provided class as key.
   * Optionally, a Map containing data to put initially in the map can be provided.
   *
   * @param keyClass
   * @param initMap
   * @param <K>
   * @param <V>
   *
   * @return
   */
  public static <K extends Enum, V> Map<K, V> newEnumMapInit(Class<K> keyClass, Optional<Map<K, V>> initMap) {
    Map<K, V> newMap = new EnumMap<>(keyClass);
    initMap.ifPresent(newMap::putAll);
    return newMap;
  }

  /**
   * see {@link #newHashMapInit(Optional)}.
   *
   * @param initMap
   * @param <K>
   * @param <V>
   * @return
   */
  public static <K, V> Map<K, V> newHashMapInit(@Nullable Map<K, V> initMap) {
    return newHashMapInit(Optional.ofNullable(initMap));
  }

  /**
   * Creates a new HashMap instance.
   * Optionally, a Map containing data to put initially in the map can be provided.
   *
   * @param initMap
   * @param <K>
   * @param <V>
   *
   * @return
   */
  public static <K, V> Map<K, V> newHashMapInit(Optional<Map<K, V>> initMap) {
    Map<K, V> newMap = new HashMap<>();
    initMap.ifPresent(newMap::putAll);
    return newMap;
  }
}
