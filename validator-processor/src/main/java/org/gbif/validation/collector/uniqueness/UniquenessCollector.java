package org.gbif.validation.collector.uniqueness;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Collects unique elements and store the duplicates found.
 */
public class UniquenessCollector {

  private final TreeSet<String> uniqueElements;
  private final HashMap<String,Integer> duplicates;

  /**
   * Default constructor.
   */
  public UniquenessCollector() {
    uniqueElements = new TreeSet<>();
    duplicates = new HashMap<>();
  }

  /**
   * Collects a value which suppose to be unique.
   */
  public void collect(String value) {
    if(!uniqueElements.add(value)) {
      duplicates.compute(value, (k, v)  -> v == null? 0 : v++);
    }
  }

  /**
   *
   * Merge the current collected data with data of another collector.
   */
  public void merge(UniquenessCollector collector) {
    Optional<String> ceiling = Optional.ofNullable(uniqueElements.ceiling(collector.uniqueElements.first()));
    if (ceiling.isPresent()) {
      Optional<String> floor = Optional.ofNullable(uniqueElements.floor(collector.uniqueElements.last()));
      if (floor.isPresent()) {
        SortedSet<String> overlapSet = uniqueElements.subSet(ceiling.get(), floor.get());
        SortedSet<String> newDuplicates = new TreeSet<>();
        collector.uniqueElements.stream().forEachOrdered(element -> {
          if (!overlapSet.contains(element)) {
            newDuplicates.add(element);
            duplicates.compute(element, (k,v) -> v == null? 0 : v++);
          }
        });
        uniqueElements.addAll(newDuplicates);
      } else {
        uniqueElements.addAll(collector.uniqueElements);
        duplicates.putAll(collector.duplicates);
      }
    } else {
      uniqueElements.addAll(collector.uniqueElements);
      duplicates.putAll(collector.duplicates);
    }
  }

  /**
   * List of unique elements.
   */
  public Set<String> getUniqueElements() {
    return uniqueElements;
  }

  /**
   * Duplicate elements.
   */
  public Map<String, Integer> getDuplicates() {
    return duplicates;
  }

}

