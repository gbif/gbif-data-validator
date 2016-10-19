package org.gbif.validation.api;

import org.gbif.validation.api.model.ValidationResult;

public interface DataFileProcessor {

  ValidationResult process(DataFile dataFile);
}
