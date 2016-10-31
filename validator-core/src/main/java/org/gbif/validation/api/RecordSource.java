package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Interface representing a source of records (file, map ...)
 */
public interface RecordSource extends Closeable {
  /**
   * Return the headers of the file
   * @return headers or null
   */
  @Nullable Term[] getHeaders();
  @Nullable String[] read() throws IOException;
}
