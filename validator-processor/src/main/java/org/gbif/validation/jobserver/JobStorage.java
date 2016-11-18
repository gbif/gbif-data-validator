package org.gbif.validation.jobserver;

import java.util.Optional;

/**
 * Interface to decouple how the job result information  is stored.
 * @param <T> type of things being stored
 */
public interface JobStorage<T> {

  /**
   * Gets the data stored of jobId.
   */
  Optional<T> get(long jobId);

  /**
   * Stores/overwrites the data of a jobId.
   */
  void put(long jobId, T data);

}
