package org.gbif.validation.api;

import org.gbif.validation.api.model.ValidationJobResponse;

public interface DataFileProcessorAsync {

  ValidationJobResponse processAsync(DataFile dataFile);
}
