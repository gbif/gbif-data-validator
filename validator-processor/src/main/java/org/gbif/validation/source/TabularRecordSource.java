package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.tabular.TabularDataFileReader;
import org.gbif.utils.file.tabular.TabularFiles;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.util.FileNormalizer;

import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * TabularRecordSource allows to expose the content of a {@link TabularDataFile} as {@link RecordSource}.
 * Warning: this class assumes {@link FileNormalizer#END_LINE} is used as end of line character.
 * Internally it wraps {@link TabularDataFileReader} to ensure compatibility with GBIF crawling.
 */
class TabularRecordSource implements RecordSource {

  private final TabularDataFile tabularDataFile;
  private final TabularDataFileReader<List<String>> tabularReader;

  TabularRecordSource(TabularDataFile tabularDataFile) throws IOException {
    Objects.requireNonNull(tabularDataFile, "tabularDataFile shall be provided");
    this.tabularDataFile = tabularDataFile;
    tabularReader = TabularFiles.newTabularFileReader(
            Files.newBufferedReader(tabularDataFile.getFilePath(), tabularDataFile.getCharacterEncoding()),
            tabularDataFile.getDelimiterChar(),
            FileNormalizer.END_LINE,
            tabularDataFile.getQuoteChar(),
            tabularDataFile.isHasHeaders());
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    return tabularDataFile.getColumns();
  }

  @Nullable
  @Override
  public List<String> read() throws IOException {
    try {
      return tabularReader.read();
    } catch (ParseException e) {
      throw new IOException("Error reading tabular data", e);
    }
  }

  @Override
  public void close() throws IOException {
    tabularReader.close();
  }
}
