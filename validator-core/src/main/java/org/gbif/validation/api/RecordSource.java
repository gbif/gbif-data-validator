package org.gbif.validation.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface representing a source of records (file, map ...)
 * @Deprecated not 100% but very likely
 */
public interface RecordSource extends Closeable {
  String[] read() throws IOException;
}
