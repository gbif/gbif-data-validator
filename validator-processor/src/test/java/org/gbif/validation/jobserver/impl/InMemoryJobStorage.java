package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.jobserver.JobStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Job storage that keeps data in a in-memory hash map.
 */
public class InMemoryJobStorage implements JobStorage {

  private final Map<Long,JobStatusResponse<?>> store = new HashMap<>();

  @Override
  public Optional<JobStatusResponse<?>> get(long jobId) {
    return Optional.ofNullable(store.get(jobId));
  }

  @Override
  public void put(JobStatusResponse<?> data) {
    store.put(data.getJobId(), data);
  }
}
