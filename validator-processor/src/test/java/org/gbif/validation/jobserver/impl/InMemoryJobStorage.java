package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.jobserver.JobStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Job storage that keeps data in a in-memory hashmap.
 */
public class InMemoryJobStorage implements JobStorage<ValidationResult> {

  private Map<Long,ValidationResult> jobStorage = new HashMap<>();

  @Override
  public Optional<ValidationResult> get(long jobId) {
    return jobStorage.containsKey(jobId) ? Optional.ofNullable(jobStorage.get(jobId)) : Optional.empty();
  }

  @Override
  public void put(long jobId, ValidationResult data) {
    jobStorage.put(jobId,data);
  }
}
