package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Interface representing a source of records (file, map ...).
 */
public interface RecordSource extends Closeable {

  /**
   * Returns the headers of the source.
   * @return headers or null
   */
  @Nullable
  Term[] getHeaders();

  /**
   * Returns the default values (if any) of the source.
   * @return
   */
  Optional<Map<Term, String>> getDefaultValues();

  @Nullable
  String[] read() throws IOException;

}
