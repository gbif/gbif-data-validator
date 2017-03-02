package org.gbif.validation.source;

import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.Validate;

import static org.gbif.utils.file.tabular.TabularFiles.newTabularFileReader;

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
   * Creates instances of RecordSource from character delimited files.
   */
  public static RecordSource fromDelimited(@NotNull File sourceFile, @NotNull Character delimiterChar,
                                           boolean headerIncluded) throws IOException {
    Objects.requireNonNull(sourceFile, "sourceFile shall be provided");
    Objects.requireNonNull(delimiterChar, "delimiterChar shall be provided");
    return new TabularFileReader(sourceFile.toPath(), newTabularFileReader(new FileInputStream(sourceFile),
                                                                           delimiterChar, headerIncluded));
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
