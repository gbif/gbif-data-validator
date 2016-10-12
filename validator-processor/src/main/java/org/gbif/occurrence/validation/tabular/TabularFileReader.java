package org.gbif.occurrence.validation.tabular;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.tabular.TermTabularDataFileReader;
import org.gbif.tabular.TermTabularDataLine;

import java.io.IOException;
import java.util.Map;

/**
 * Temporary class
 */
public class TabularFileReader implements RecordSource {

  private final TermTabularDataFileReader wrapped;

  public TabularFileReader(TermTabularDataFileReader wrapped){
    this.wrapped = wrapped;
  }

  @Override
  public Map<Term, String> read() throws IOException {
    TermTabularDataLine row = wrapped.read();
    if(row != null) {
      return row.getMappedData();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
