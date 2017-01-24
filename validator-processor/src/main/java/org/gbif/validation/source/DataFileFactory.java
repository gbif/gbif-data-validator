package org.gbif.validation.source;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileBashUtilities;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.Validate;

import static org.gbif.validation.source.RecordSourceFactory.fromDataFile;

/**
 *
 */
public class DataFileFactory {

  /**
   * Private constructor.
   */
  private DataFileFactory() {
    //empty method
  }

  /**
   * Creates a new {@link DataFile} with the minimum information required.
   *
   * @param filePath
   * @param sourceFileName
   * @param fileFormat
   * @param contentType
   * @return
   */
  public static DataFile newDataFile(Path filePath, String sourceFileName, FileFormat fileFormat,
                                        String contentType) {
    DataFile dataFile = new DataFile();
    dataFile.setFilePath(filePath);
    dataFile.setFileFormat(fileFormat);
    dataFile.setSourceFileName(sourceFileName);
    dataFile.setContentType(contentType);

    return dataFile;
  }

  /**
   * Prepare the {@link DataFile} for evaluation.
   * This step includes reading the headers from the file and generating a list of {@link DataFile}.
   *
   * @param dataFile
   * @return a list of {@link DataFile}
   * @throws IOException
   * @throws IllegalStateException
   */
  public static List<DataFile> prepareDataFile(DataFile dataFile) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");

    List<DataFile> dataFileList = new ArrayList<>();
    switch (dataFile.getFileFormat()) {
      case SPREADSHEET:
        dataFileList.add(handleSpreadsheetConversion(dataFile));
        break;
      case DWCA:
        dataFileList.addAll(prepareDwcA(dataFile));
        break;
      case TABULAR:
        dataFileList.add(prepareTabular(dataFile));
    }

    for (DataFile currDataFile : dataFileList) {
      currDataFile.setNumOfLines(FileBashUtilities.countLines(currDataFile.getFilePath().toAbsolutePath().toString()));
      try (RecordSource rs = fromDataFile(currDataFile).orElse(null)) {
        if (rs != null) {
          currDataFile.setColumns(rs.getHeaders());
          currDataFile.setDefaultValues(rs.getDefaultValues());

          //if the rowType is not provided and we have headers we can try to guess it
          if (currDataFile.getColumns() != null && currDataFile.getRowType() == null) {
            currDataFile.setRowType(determineRowType(Arrays.asList(currDataFile.getColumns())).orElse(null));
          }
        }
      }
    }

    return dataFileList;
  }

  /**
   * Given a {@link DataFile} pointing to folder containing the extracted DarwinCore archive this method creates
   * a list of {@link DataFile} for each of the data component (core + extensions).
   *
   * @param dwcaDataFile
   * @return
   */
  private static List<DataFile> prepareDwcA(DataFile dwcaDataFile) throws IOException {
    Validate.isTrue(dwcaDataFile.getFilePath().toFile().isDirectory(),
            "dwcaDataFile.getFilePath() must point to a directory");
    List<DataFile> dataFileList = new ArrayList<>();
    try (DwcReader dwcReader = new DwcReader(dwcaDataFile.getFilePath().toFile())) {
      //add the core first
      DataFile core = createDwcDataFile(dwcaDataFile, dwcReader.getCore().getLocationFile().toPath());
      core.setRowType(dwcReader.getRowType());
      core.setType(DwcFileType.CORE);
      core.setHasHeaders(dwcReader.getCore().getIgnoreHeaderLines() != null
              && dwcReader.getCore().getIgnoreHeaderLines() > 0);
      core.setDelimiterChar(dwcReader.getCore().getFieldsTerminatedBy().charAt(0));
      dataFileList.add(core);

      for (ArchiveFile ext : dwcReader.getExtensions()) {
        DataFile extDatafile = createDwcDataFile(dwcaDataFile, Paths.get(ext.getLocationFile().getAbsolutePath()));
        extDatafile.setRowType(ext.getRowType());
        extDatafile.setType(DwcFileType.EXTENSION);
        extDatafile.setHasHeaders(ext.getIgnoreHeaderLines() != null && ext.getIgnoreHeaderLines() > 0);
        extDatafile.setDelimiterChar(ext.getFieldsTerminatedBy().charAt(0));
        dataFileList.add(extDatafile);
      }
    }
    return dataFileList;
  }

  /**
   *
   * @param dwcaDataFile
   * @return
   * @throws IOException
   */
  private static DataFile prepareTabular(DataFile dwcaDataFile) throws IOException {

    if (dwcaDataFile.getDelimiterChar() == null) {
      dwcaDataFile.setDelimiterChar(getDelimiter(dwcaDataFile.getFilePath()));
    }
    return dwcaDataFile;
  }

  /**
   * Creates a new {@link DataFile} representing a DarwinCore component.
   *
   * @param dwcaDatafile
   * @param dwcComponentPath
   * @return
   */
  private static DataFile createDwcDataFile(DataFile dwcaDatafile, Path dwcComponentPath) {

    DataFile dwcComponentDataFile = new DataFile(dwcaDatafile);
    dwcComponentDataFile.setFilePath(dwcComponentPath);
    dwcComponentDataFile.setSourceFileName(dwcComponentPath.getFileName().toString());
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

    Path csvFile = spreadsheetFile.getParent().resolve(UUID.randomUUID() + ".csv");
    if (ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET.equalsIgnoreCase(contentType) ||
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

  /**
   * Tries to determine the rowType of a file based on its headers.
   *
   * @param headers
   * @return
   */
  private static Optional<Term> determineRowType(List<Term> headers) {
    if (headers.contains(DwcTerm.occurrenceID)) {
      return Optional.of(DwcTerm.Occurrence);
    }
    if (headers.contains(DwcTerm.taxonID)) {
      return Optional.of(DwcTerm.Taxon);
    }
    if (headers.contains(DwcTerm.eventID)) {
      return Optional.of(DwcTerm.Event);
    }
    return Optional.empty();
  }

  /**
   * Guesses the delimiter character form the data file.
   * @throws UnkownDelimitersException
   */
  private static Character getDelimiter(Path dataFilePath) {
    CSVReaderFactory.CSVMetadata metadata = CSVReaderFactory.extractCsvMetadata(dataFilePath.toFile(), "UTF-8");
    if (metadata.getDelimiter().length() == 1) {
      return metadata.getDelimiter().charAt(0);
    } else {
      throw new UnkownDelimitersException(metadata.getDelimiter() + "{} is a non supported delimiter");
    }
  }


}
