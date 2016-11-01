package org.gbif.validation.api;

import org.gbif.validation.api.model.ValidationResult;

import java.io.IOException;

public interface DataFileProcessor {

  ValidationResult process(DataFile dataFile) throws IOException;
}
