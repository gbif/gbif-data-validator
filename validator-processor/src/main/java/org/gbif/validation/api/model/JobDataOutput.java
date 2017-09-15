package org.gbif.validation.api.model;

import org.gbif.validation.api.result.ValidationDataOutput;

import java.util.Objects;

/**
 * Structure to hold data output of a validation job.
 * Since the output data can be a lot of things we only keep it as Object.
 */
public class JobDataOutput {

  private long jobId;
  private ValidationDataOutput.Type type;
  private Object content;

  public JobDataOutput(long jobId, ValidationDataOutput.Type type, Object content) {
    this.jobId = jobId;
    this.type = type;
    this.content = content;
  }

  public JobDataOutput(long jobId, ValidationDataOutput validationDataOutput) {
    Objects.requireNonNull(validationDataOutput, "validationDataOutput shall be provided");

    this.jobId = jobId;
    this.type = validationDataOutput.getType();
    this.content = validationDataOutput.getContent();
  }

  /**
   * Job identifier.
   */
  public long getJobId() {
    return jobId;
  }

  public ValidationDataOutput.Type getType() {
    return type;
  }

  public Object getContent() {
    return content;
  }
}
