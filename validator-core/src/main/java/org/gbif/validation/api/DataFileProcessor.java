package org.gbif.validation.api;

import org.gbif.validation.api.result.RecordsValidationResultElement;

public interface DataFileProcessor {

  RecordsValidationResultElement process(DataFile dataFile);
}
