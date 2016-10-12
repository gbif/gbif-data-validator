package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.utils.file.tabular.TabularDataFileReader;

import java.io.IOException;
import java.util.List;

/**
 * Probably a Temporary class
 */
public class TabularFileReader implements RecordSource {

  private final TabularDataFileReader<List<String>> wrapped;

  public TabularFileReader(TabularDataFileReader<List<String>> wrapped){
    this.wrapped = wrapped;
  }

  @Override
  public String[] read() throws IOException {
    List<String> row = wrapped.read();
    if(row != null) {
      return row.toArray(new String[0]);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
