package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.tabular.TabularDataFileReader;
import org.gbif.utils.file.tabular.TabularFiles;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import javax.annotation.Nullable;

/**
 * TabularRecordSource allows to expose the content of a {@link TabularDataFile} as {@link RecordSource}.
 * Internally it wraps {@link TabularDataFileReader} to ensure compatibility with GBIF crawling.
 */
class TabularRecordSource implements RecordSource {

  private final TabularDataFile tabularDataFile;
  private final TabularDataFileReader<List<String>> tabularReader;

  TabularRecordSource(TabularDataFile tabularDataFile) throws IOException {
    this.tabularDataFile = tabularDataFile;
    tabularReader = TabularFiles.newTabularFileReader(
            Files.newBufferedReader(tabularDataFile.getFilePath(),
                    tabularDataFile.getCharacterEncoding()), tabularDataFile.getDelimiterChar(),
            tabularDataFile.isHasHeaders());
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    return tabularDataFile.getColumns();
  }

  @Nullable
  @Override
  public String[] read() throws IOException {
    List<String> line = tabularReader.read();

    if(line == null){
      return null;
    }
    //FIXME we should probably return the List here
    return line.toArray(new String[line.size()]);
  }

  @Override
  public void close() throws IOException {
    tabularReader.close();
  }
}
