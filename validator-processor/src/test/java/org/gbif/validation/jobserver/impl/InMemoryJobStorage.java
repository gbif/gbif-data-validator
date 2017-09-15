package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.JobDataOutput;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.jobserver.JobStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Job storage that keeps data in a in-memory hash map.
 */
public class InMemoryJobStorage implements JobStorage {

  private final Map<Long, JobStatusResponse<?>> store = new HashMap<>();
  private final Map<Long, JobDataOutput> dataOutputStore = new HashMap<>();

  @Override
  public Optional<JobStatusResponse<?>> getStatus(long jobId) {
    return Optional.ofNullable(store.get(jobId));
  }

  @Override
  public Optional<JobDataOutput> getDataOutput(long jobId, JobDataOutput.Type Type) throws IOException {
    return null;
  }

  @Override
  public void put(JobStatusResponse<?> data) {
    store.put(data.getJobId(), data);
  }

  /**
   * Supports only 1 type per job
   * @param data
   */
  @Override
  public void put(JobDataOutput data) {
    dataOutputStore.put(data.getJobId(), data);
  }
}
