package org.gbif.validation.source;

import org.gbif.dwca.io.ArchiveFile;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileBashUtilities;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

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
        if(dataFile.getFileLineOffset().isPresent()){
          return Optional.of(new DwcReader(dataFile.isAlternateViewOf().get().getFilePath().toFile(),
                  dataFile.getFilePath().toFile(), dataFile.getRowType(), dataFile.isHasHeaders()));
        }
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

    List<DataFile> dataFileList = new ArrayList<>();

    switch(dataFile.getFileFormat()) {
      case SPREADSHEET:
        dataFileList.add(handleSpreadsheetConversion(dataFile));
          break;
      case DWCA:
        dataFileList.addAll(prepareDwcA(dataFile));
        break;
      case TABULAR:
      default :
        dataFileList.add(dataFile);
    }

    for(DataFile currDataFile : dataFileList) {
      currDataFile.setNumOfLines(FileBashUtilities.countLines(currDataFile.getFilePath().toAbsolutePath().toString()));
      try (RecordSource rs = fromDataFile(currDataFile).orElse(null)) {
        if (rs != null) {
          currDataFile.setColumns(rs.getHeaders());
        }
      }
    }

    return dataFileList;
  }

  /**
   * Given a {@link DataFile} pointing to folder containing the extracted DarwinCore archive this method creates
   * a list of {@link DataFile} for each of the data component (core + extensions).
   * @param dwcaDataFile
   * @return
   */
  private static final List<DataFile> prepareDwcA(DataFile dwcaDataFile) throws IOException {
    Validate.isTrue(dwcaDataFile.getFilePath().toFile().isDirectory(), "dwcaDataFile.getFilePath() must point to a directory");
    List<DataFile> dataFileList = new ArrayList<>();
    DwcReader dwcReader = new DwcReader(dwcaDataFile.getFilePath().toFile());

    //add the core first
    DataFile core = createDwcDataFile(dwcaDataFile, dwcReader.getFileSource());
    core.setRowType(dwcReader.getRowType());
    dataFileList.add(core);

    DataFile extDatafile;
    for(ArchiveFile ext : dwcReader.getExtensions()){
      extDatafile = createDwcDataFile(dwcaDataFile, Paths.get(ext.getLocationFile().getAbsolutePath()));
      extDatafile.setRowType(ext.getRowType());
      dataFileList.add(extDatafile);
    }
    return dataFileList;
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
