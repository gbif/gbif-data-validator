package org.gbif.validation.jobserver.messages;

/**
 * Encapsulates the work to be done by a Job.
 *
 * @param <T> type of data work to do
 */
public class DataJob<T> {

  private final long jobId;

  private final T jobData;

  /**
   * Full constructor.
   */
  public DataJob(long jobId, T jobData) {
    this.jobId = jobId;
    this.jobData = jobData;
  }

  /**
   * Gets the unique job identifier.
   */
  public long getJobId() {
    return jobId;
  }

  /**
   * Gets the Job's data work to be done.
   */
  public T getJobData() {
    return jobData;
  }
}
