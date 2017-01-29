package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.Validate;

import static org.gbif.utils.file.tabular.TabularFiles.newTabularFileReader;

/**
 * Creates instances of RecordSource class.
 *
 * RecordSourceFactory contract is to provide a RecordSource from different source therefore
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
   * Creates instance of RecordSource from a folder containing an extracted DarwinCore archive.
   */
  public static RecordSource fromDwcA(@NotNull File sourceFolder) throws IOException {
    Objects.requireNonNull(sourceFolder, "sourceFolder shall be provided");
    return new DwcReader(sourceFolder);
  }

  /**
   * Creates instance of RecordSource from a folder containing an extracted DarwinCore archive and using a specific
   * component (rowType) of the Archive.
   * @param sourceFolder
   * @param rowType
   * @return
   * @throws IOException
   */
  public static RecordSource fromDwcA(@NotNull File sourceFolder, @NotNull Term rowType) throws IOException {
    Objects.requireNonNull(sourceFolder, "sourceFolder shall be provided");
    Objects.requireNonNull(rowType, "rowType shall be provided");

    return new DwcReader(sourceFolder, Optional.of(rowType));
  }


  /**
   * Build a new RecordSource matching for the {@link TabularDataFile} file format.
   * This method will only return a RecordSource for TABULAR or DWCA.
   *
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static Optional<RecordSource> fromTabularDataFile(TabularDataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    Validate.validState(FileFormat.SPREADSHEET != dataFile.getFileFormat(),
            "FileFormat.SPREADSHEET can not be read directly. Use prepareSource().");
    Validate.validState(FileFormat.TABULAR != dataFile.getFileFormat() || dataFile.getDelimiterChar() != null,
            "FileFormat.TABULAR shall also provide delimiterChar");
    if (FileFormat.TABULAR == dataFile.getFileFormat()) {
      return Optional.of(fromDelimited(dataFile.getFilePath().toFile(), dataFile.getDelimiterChar(),
                                       dataFile.isHasHeaders()));
    }
    if (FileFormat.DWCA == dataFile.getFileFormat()) {
      File dwcaFolder = dataFile.getMetadataFolder().map(Path::toFile).orElse(null);
      //line offset means this file is a portion of the entire file
      if (dataFile.getFileLineOffset().isPresent()) {
        return Optional.of(new DwcReader(dwcaFolder,
                dataFile.getFilePath().toFile(), dataFile.getRowType(), dataFile.isHasHeaders()));
      }
      return Optional.of(fromDwcA(dwcaFolder, dataFile.getRowType()));
    }
    return Optional.empty();
  }

}
