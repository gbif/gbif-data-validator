package org.gbif.validation.api;

import org.gbif.validation.api.model.JobStatusResponse;

public interface DataFileProcessorAsync {

  JobStatusResponse processAsync(DataFile dataFile);
}
