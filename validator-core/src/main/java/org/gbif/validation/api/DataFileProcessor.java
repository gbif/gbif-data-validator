package org.gbif.validation.api;

import org.gbif.validation.api.result.RecordsValidationResultElement;

import java.io.IOException;

public interface DataFileProcessor {

  RecordsValidationResultElement process(DataFile dataFile) throws IOException;
}
