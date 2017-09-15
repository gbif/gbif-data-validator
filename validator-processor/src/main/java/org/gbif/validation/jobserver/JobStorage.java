package org.gbif.validation.jobserver;

import org.gbif.validation.api.model.JobDataOutput;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.result.ValidationDataOutput;

import java.io.IOException;
import java.util.Optional;

/**
 * Interface to decouple how the job result information  is stored.
 */
public interface JobStorage {

  /**
   * Gets the data stored of jobId.
   */
  Optional<JobStatusResponse<?>> getStatus(long jobId) throws IOException;

  /**
   * Get data output for a specific jobId and type of output.
   *
   * @param jobId
   * @param type
   *
   * @return the JobDataOutput found or Optional.empty if not
   *
   * @throws IOException
   */
  Optional<JobDataOutput> getDataOutput(long jobId, ValidationDataOutput.Type type) throws IOException;

  /**
   * Stores/overwrites the data of a jobId.
   */
  void put(JobStatusResponse<?> response);

  /**
   * Stores/overwrites the output data of a jobId.
   */
  void put(JobDataOutput dataOutput);

}
