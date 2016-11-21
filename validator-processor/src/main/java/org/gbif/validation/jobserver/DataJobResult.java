package org.gbif.validation.jobserver;

import java.util.Optional;

/**
 * Result of a finished data job.
 *
 * @param <T> type of data job to be done
 */
public class DataJobResult<T> {

  private final DataJob<?> dataJob;

  private final Optional<T> result;

  /**
   * Full constructor.
   */
  public DataJobResult(DataJob<?> dataJob, T result) {
    this.dataJob = dataJob;
    this.result = Optional.ofNullable(result);
  }

  /**
   * Data job performed.
   */
  public DataJob<?> getDataJob() {
    return dataJob;
  }

  /**
   * Resulting data performed on the dataJob input.
   */
  public Optional<T> getResult() {
    return result;
  }
}
