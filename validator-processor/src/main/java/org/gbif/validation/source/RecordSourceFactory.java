package org.gbif.validation.source;

import org.gbif.utils.file.tabular.TabularFiles;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.lang3.Validate;

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
  public static RecordSource fromDelimited(File sourceFile, char delimiterChar, boolean headerIncluded)
          throws IOException {
    return new TabularFileReader(sourceFile.toPath(), TabularFiles.newTabularFileReader(new FileInputStream(sourceFile), delimiterChar,
            headerIncluded));
  }

  /**
   * Creates instances of RecordSource from a folder containing an extracted DarwinCore archive.
   */
  public static RecordSource fromDwcA(File sourceFolder)
          throws IOException {
    return new DwcReader(sourceFolder);
  }

  /**
   * Build a new RecordSource matching the {@link DataFile} file format.
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static RecordSource fromDataFile(DataFile dataFile) throws IOException {
    Validate.notNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    switch (dataFile.getFileFormat()) {
      case TABULAR : return
              fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(), dataFile.isHasHeaders());
      case DWCA: return fromDwcA(new File(dataFile.getFileName()));
    }
    return null;
  }

  /**
   * Prepare the source for reading.
   * This step includes reading the headers from the file.
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static DataFile prepareSource(DataFile dataFile) throws IOException {

    Validate.notNull(dataFile.getFileName(), "fileName shall be provided");
    Validate.notNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    RecordSource rs = fromDataFile(dataFile);
    if(rs != null){
      dataFile.setNumOfLines(FileBashUtilities.countLines(rs.getFileSource().toAbsolutePath().toString()));
      dataFile.setColumns(rs.getHeaders());

      if(FileFormat.DWCA.equals(dataFile.getFileFormat())) {
        dataFile.setRowType(((DwcReader)rs).getRowType());
        //change the current file path to point to the core
        dataFile.setFileName(rs.getFileSource().toString());
      }

      rs.close();
    }
    return dataFile;
  }

}
