package org.gbif.validation.source;

import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;

import java.io.IOException;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Creates instances of {@link RecordSource} class.
 *
 * {@link RecordSourceFactory} contract is to provide a RecordSource from {@link TabularDataFile}, therefore
 * no validation on the structure of the source will be performed.
 */
public class RecordSourceFactory {

  /**
   * Private constructor.
   */
  private RecordSourceFactory() {
    //empty method
  }

  /**
   * Build a new RecordSource from a {@link TabularDataFile}.
   *
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static RecordSource fromTabularDataFile(TabularDataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");

    Preconditions.checkArgument(dataFile.getCharacterEncoding() != null, "characterEncoding shall be set");
    Preconditions.checkArgument(dataFile.getDelimiterChar() != null, "delimiterChar shall be set");

    return new TabularRecordSource(dataFile);
  }

}
