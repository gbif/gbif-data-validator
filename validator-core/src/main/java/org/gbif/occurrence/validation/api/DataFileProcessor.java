package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.api.model.DataFileValidationResult;

public interface DataFileProcessor {

  DataFileValidationResult process(DataFile dataFile);
}
