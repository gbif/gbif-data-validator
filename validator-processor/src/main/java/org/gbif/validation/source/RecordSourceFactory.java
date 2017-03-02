package org.gbif.validation.source;

import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang3.Validate;

/**
 * Creates instances of RecordSource class.
 *
 * RecordSourceFactory contract is to provide a RecordSource from {@link TabularDataFile}, therefore
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
   * This method will only return a RecordSource for TABULAR or DWCA.
   *
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static RecordSource fromTabularDataFile(TabularDataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    Validate.validState(FileFormat.SPREADSHEET != dataFile.getFileFormat(),
            "FileFormat.SPREADSHEET can not be read directly. Use prepareSource().");

    Validate.validState(FileFormat.TABULAR != dataFile.getFileFormat() || dataFile.getDelimiterChar() != null,
            "FileFormat.TABULAR shall also provide delimiterChar");

    return new TabularRecordSource(dataFile);
  }

}
