package org.gbif.validation.api.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Response of job status request.
 * @param <T> type of the content embedded in this response
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class JobStatusResponse<T> {

  /**
   * Enumerates the possible statuses of Job.
   */
  public enum JobStatus {
    ACCEPTED, RUNNING, FAILED, FINISHED, NOT_FOUND, KILLED;
  }

  //Static object that represents an error processing a job
  public static final JobStatusResponse<?> FAILED_RESPONSE = new JobStatusResponse(JobStatus.FAILED, -1L);
  //Static object to be used when a requested job id is not found in the job storage
  public static final JobStatusResponse<?> NOT_FOUND_RESPONSE = new JobStatusResponse(JobStatus.NOT_FOUND, -1L);

  @JsonProperty
  private JobStatus status;
  @JsonProperty
  private long jobId;
  @JsonProperty
  private T result;

  /**
   * Empty constructor required for serialization.
   */
  public JobStatusResponse() {
    //empty constructor
  }

  /**
   * Full constructor.
   */
  public JobStatusResponse(JobStatus status, long jobId, T result) {
    this.status = status;
    this.jobId = jobId;
    this.result = result;
  }

  /**
   * Constructor to build partial responses, i.e.: without results.
   */
  public JobStatusResponse(JobStatus status, long jobId) {
    this.status = status;
    this.jobId = jobId;
  }

  /**
   * Job status.
   */
  public JobStatus getStatus() {
    return status;
  }

  /**
   * Job identifier.
   */
  public long getJobId() {
    return jobId;
  }

  /**
   * Stored result for this job.
   */
  public T getResult() {
    return result;
  }
}
