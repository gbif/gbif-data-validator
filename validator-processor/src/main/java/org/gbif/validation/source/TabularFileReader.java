package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordSource;
import org.gbif.utils.file.tabular.TabularDataFileReader;
import org.gbif.validation.util.TempTermsUtils;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Probably a Temporary class
 */
public class TabularFileReader implements RecordSource {

  private List<String> headerLine;
  private final TabularDataFileReader<List<String>> wrapped;

  TabularFileReader(TabularDataFileReader<List<String>> wrapped) throws IOException {
    this.wrapped = wrapped;
    headerLine = wrapped.getHeaderLine();
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    if(headerLine != null){
      return TempTermsUtils.buildTermMapping(headerLine.toArray(new String[headerLine.size()]));
    }
    return null;
  }

  @Override
  public String[] read() throws IOException {
    List<String> row = wrapped.read();
    if(row != null) {
      return row.toArray(new String[row.size()]);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
