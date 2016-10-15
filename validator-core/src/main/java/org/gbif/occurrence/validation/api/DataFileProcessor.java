package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.api.model.ValidationResult;

public interface DataFileProcessor {

  ValidationResult process(DataFile dataFile);
}
