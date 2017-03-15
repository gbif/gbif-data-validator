package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.csv.CSVReader;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * TabularRecordSource allows to expose the content of a {@link TabularDataFile} as {@link RecordSource}.
 * Internally it wraps {@link CSVReader} to ensure compatibility with GBIF crawling.
 */
class TabularRecordSource implements RecordSource {

  private final TabularDataFile tabularDataFile;
  private final CSVReader csvReader;

  TabularRecordSource(TabularDataFile tabularDataFile) throws IOException {
    this.tabularDataFile = tabularDataFile;

    csvReader = new CSVReader(tabularDataFile.getFilePath().toFile(),
            tabularDataFile.getCharacterEncoding().name(),
            tabularDataFile.getDelimiterChar().toString(),
            tabularDataFile.getQuoteChar(),
            tabularDataFile.isHasHeaders() ? 1 : 0);
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    return tabularDataFile.getColumns();
  }

  @Nullable
  @Override
  public String[] read() throws IOException {
    return csvReader.next();
  }

  @Override
  public void close() throws IOException {
    csvReader.close();
  }
}
