package org.gbif.validation.api.model;

import java.util.UUID;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Response of job status request.
 * @param <T> type of the content embedded in this response
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobStatusResponse<T> {

  /**
   * Enumerates the possible statuses of Job.
   */
  public enum JobStatus {
    ACCEPTED(false), RUNNING(false),
    FAILED(true), FINISHED(true), NOT_FOUND(true), KILLED(true);

    private boolean _final;

    JobStatus(boolean _final) {
      this._final = _final;
    }

    /**
     * Indicates if the state is expected to change or is the result is final.
     *
     * @return
     */
    public boolean isFinal() {
      return _final;
    }
  }

  //Static object that represents an error processing a job
  public static final JobStatusResponse<?> FAILED_RESPONSE = new JobStatusResponse(JobStatus.FAILED, -1L, null, null);

  @JsonProperty
  private Long startTimestamp;
  //private final long endTimestamp;

  @JsonProperty
  private JobStatus status;

  @JsonProperty
  private long jobId;

  @JsonProperty
  private UUID dataFileKey;

  @JsonProperty
  private T result;


  /**
   * Create a {@link JobStatusResponse} representing a jobId not found.
   * @param jobIdNotFound
   * @return
   */
  public static JobStatusResponse ofNotFound(long jobIdNotFound) {
    return new JobStatusResponse(JobStatus.NOT_FOUND, jobIdNotFound, null, null);
  }

  public static JobStatusResponse ofAccepted(long jobId, long startTimestamp, UUID dataFileKey) {
    return new JobStatusResponse(JobStatus.ACCEPTED, jobId, startTimestamp, dataFileKey);
  }

  /**
   * Create a {@link JobStatusResponse} representing a running job.
   * @param jobId
   * @return
   */
  public static JobStatusResponse ofRunning(long jobId, long startTimestamp, UUID dataFileKey) {
    return new JobStatusResponse(JobStatus.RUNNING, jobId, startTimestamp, dataFileKey);
  }

  /**
   * Create a {@link JobStatusResponse} representing a killed job.
   * @param jobId
   * @param dataFileKey
   * @return
   */
  public static JobStatusResponse ofKilled(long jobId, long startTimestamp, UUID dataFileKey) {
    return new JobStatusResponse(JobStatus.KILLED, jobId, startTimestamp, dataFileKey);
  }

  /**
   * Empty constructor required for serialization.
   */
  public JobStatusResponse() {
    //empty constructor
  }

  /**
   * Full constructor.
   */
  public JobStatusResponse(JobStatus status, long jobId, Long startTimestamp, UUID dataFileKey, T result) {
    this.status = status;
    this.jobId = jobId;
    this.startTimestamp = startTimestamp;
    this.dataFileKey = dataFileKey;
    this.result = result;
  }

  /**
   * Constructor to build partial responses, i.e.: without results.
   */
  public JobStatusResponse(JobStatus status, long jobId, Long startTimestamp, UUID dataFileKey) {
    this(status, jobId, startTimestamp, dataFileKey, null);
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

  public Long getStartTimestamp() {
    return startTimestamp;
  }

  public UUID getDataFileKey() {
    return dataFileKey;
  }

  /**
   * Stored result for this job.
   */
  public T getResult() {
    return result;
  }

}
