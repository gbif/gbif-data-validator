package org.gbif.validation.api;

import java.io.IOException;

public interface DataFileProcessor {

  void process(DataFile dataFile) throws IOException;
}
