package org.gbif.validation.api.model;

import org.gbif.validation.api.result.ValidationResult;

public class ValidationJobResponse {

  public enum JobStatus {
    ACCEPTED, RUNNING, FAILED, FINISHED, NOT_FOUND, KILLED;
  }

  public static final ValidationJobResponse FAILED_RESPONSE = new ValidationJobResponse(JobStatus.FAILED,-1L);
  public static final ValidationJobResponse NOT_FOUND = new ValidationJobResponse(JobStatus.NOT_FOUND,-1L);

  private JobStatus status;
  private long jobId;
  private ValidationResult result;

  /**
   * Empty constructor required for serialization.
   */
  public ValidationJobResponse() {

  }

  public ValidationJobResponse(JobStatus status, long jobId, ValidationResult result) {
    this.status = status;
    this.jobId = jobId;
    this.result = result;
  }

  public ValidationJobResponse(JobStatus status, long jobId) {
    this.status = status;
    this.jobId = jobId;
    result = null;
  }

  public JobStatus getStatus() {
    return status;
  }

  public long getJobId() {
    return jobId;
  }

  public ValidationResult getResult() {
    return result;
  }
}
