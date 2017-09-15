package org.gbif.validation.api.model;

/**
 * Structure to hold data output of a validation job.
 * Since the output data can be a lot of things we only keep it as Object.
 */
public class JobDataOutput {

  /**
   * Enumerates the possible statuses of Job.
   */
  public enum Type {
    DATASET_OBJECT;
  }

  private long jobId;
  private Type type;
  private Object content;

  public JobDataOutput(long jobId, Type type, Object content) {
    this.jobId = jobId;
    this.type = type;
    this.content = content;
  }

  /**
   * Job identifier.
   */
  public long getJobId() {
    return jobId;
  }

  public Type getType() {
    return type;
  }

  public Object getContent() {
    return content;
  }
}
