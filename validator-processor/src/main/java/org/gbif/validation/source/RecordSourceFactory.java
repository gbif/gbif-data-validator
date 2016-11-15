package org.gbif.validation.source;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileBashUtilities;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

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
  public static RecordSource fromDelimited(File sourceFile, char delimiterChar, boolean headerIncluded)
          throws IOException {
    return new TabularFileReader(sourceFile.toPath(), newTabularFileReader(new FileInputStream(sourceFile),
                                                                           delimiterChar, headerIncluded));
  }

  /**
   * Creates instances of RecordSource from a folder containing an extracted DarwinCore archive.
   */
  public static RecordSource fromDwcA(File sourceFolder) throws IOException {
    return new DwcReader(sourceFolder);
  }

  /**
   * Build a new RecordSource matching the {@link DataFile} file format.
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static Optional<RecordSource> fromDataFile(DataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    switch (dataFile.getFileFormat()) {
      case TABULAR:
        return
                Optional.of(fromDelimited(dataFile.getFilePath().toFile(), dataFile.getDelimiterChar(),
                        dataFile.isHasHeaders()));
      case DWCA:
        return Optional.of(fromDwcA(dataFile.getFilePath().toFile()));
    }
    return Optional.empty();
  }

  /**
   * Prepare the source for reading.
   * This step includes reading the headers from the file and generating a list of {@link DataFile}.
   *
   * @param dataFile
   * @return
   * @throws IOException
   */
  public static List<DataFile> prepareSource(DataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    DataFile workingDataFile = dataFile;

    //handle file conversion
    if(FileFormat.SPREADSHEET == dataFile.getFileFormat()) {
      workingDataFile = handleSpreadsheetConversion(dataFile);
    }

    try (RecordSource rs = fromDataFile(workingDataFile).orElse(null)) {
      if (rs != null) {
        workingDataFile.setNumOfLines(FileBashUtilities.countLines(rs.getFileSource().toAbsolutePath().toString()));
        workingDataFile.setColumns(rs.getHeaders());

        if (FileFormat.DWCA == workingDataFile.getFileFormat()) {
          //we only work with the core for now
          workingDataFile = createDwcDataFile(workingDataFile, rs.getFileSource());
          workingDataFile.setSourceFileName(rs.getFileSource().getFileName().toString());
          workingDataFile.setRowType(((DwcReader) rs).getRowType());
        }
      }
    }

    // the list will be used to return extensions
    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.add(workingDataFile);
    return dataFiles;
  }

  /**
   * 
   * @param dwcaDatafile
   * @param componentPath
   * @return
   */
  private static DataFile createDwcDataFile(DataFile dwcaDatafile, Path componentPath) {

    DataFile dwcComponentDataFile = new DataFile(dwcaDatafile);
    dwcComponentDataFile.setFilePath(componentPath);

    dwcComponentDataFile.setNumOfLines(dwcaDatafile.getNumOfLines());
    dwcComponentDataFile.setColumns(dwcaDatafile.getColumns());
    dwcComponentDataFile.setContentType(dwcaDatafile.getContentType());
    dwcComponentDataFile.setFileFormat(dwcaDatafile.getFileFormat());

    return dwcComponentDataFile;
  }

  /**
   *
   * @return new DataFile instance representing the converted file
   * @throws IOException
   */
  private static DataFile handleSpreadsheetConversion(DataFile spreadsheetDataFile)
          throws IOException {

    Path spreadsheetFile = spreadsheetDataFile.getFilePath();
    String contentType = spreadsheetDataFile.getContentType();

    Path csvFile = spreadsheetFile.getParent().resolve(UUID.randomUUID().toString() + ".csv");
    if(ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET.equalsIgnoreCase(contentType) ||
            ExtraMediaTypes.APPLICATION_EXCEL.equalsIgnoreCase(contentType)) {
      SpreadsheetConverters.convertExcelToCSV(spreadsheetFile, csvFile);
    } else if (ExtraMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET.equalsIgnoreCase(contentType)) {
      SpreadsheetConverters.convertOdsToCSV(spreadsheetFile, csvFile);
    }

    DataFile dataFile = new DataFile(spreadsheetDataFile);
    dataFile.setFilePath(csvFile);
    dataFile.setHasHeaders(true);
    dataFile.setDelimiterChar(',');
    dataFile.setContentType(ExtraMediaTypes.TEXT_CSV);
    dataFile.setFileFormat(FileFormat.TABULAR);

    return dataFile;
  }

}
