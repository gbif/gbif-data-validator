package org.gbif.validation.source;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileNormalizer;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Main factory used to create and prepare {@link DataFile} and {@link TabularDataFile}.
 */
public class DataFileFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DataFileFactory.class);

  /**
   * Predefined mapping between {@link Term} and its rowType.
   * Ordering is important since the first found will be used.
   */
  private static final Map<Term, Term> TERM_TO_ROW_TYPE;
  static {
    Map<Term, Term> idToRowType = new LinkedHashMap<>();
    idToRowType.put(DwcTerm.eventID, DwcTerm.Event);
    idToRowType.put(DwcTerm.taxonID, DwcTerm.Taxon);
    idToRowType.put(DwcTerm.occurrenceID, DwcTerm.Occurrence);
    TERM_TO_ROW_TYPE = Collections.unmodifiableMap(idToRowType);
  }

  private static final String CSV_EXT = ".csv";

  /**
   * Terms that can represent an identifier within a file
   */
  private static final List<Term> ID_TERMS = Collections.unmodifiableList(
          Arrays.asList(DwcTerm.eventID, DwcTerm.occurrenceID, DwcTerm.taxonID, DcTerm.identifier));
  /**
   * Recognized sheet names in Excel workbook when the workbook contains more than 1 sheet.
   * Mostly name of sheets used in GBIF provided templates.
   */
  private static final List<String> KNOWN_EXCEL_SHEET_NAMES = Collections.unmodifiableList(
          Arrays.asList("sampling events", "classification", "occurrences"));

  /**
   * Function to select an Excel sheet within multiple sheets.
   * This will select the first known sheet starting from the left.
   */
  private static final Function<List<String>, Optional<String>> SELECT_EXCEL_SHEET =
          sheetsList -> sheetsList.stream()
                  .filter(name -> KNOWN_EXCEL_SHEET_NAMES.contains(name.toLowerCase()))
                  .findFirst();

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
    return new DataFile(filePath, sourceFileName, fileFormat, contentType);
  }

  /**
   *
   * @param tabDatafile
   * @param splitFilePath
   * @param lineOffset
   * @param withHeader
   * @return
   */
  public static TabularDataFile newTabularDataFileSplit(TabularDataFile tabDatafile, Path splitFilePath,
                                                        Optional<Integer> lineOffset, boolean withHeader) {
    //FIXME lineNumber
    return new TabularDataFile(splitFilePath,
            tabDatafile.getSourceFileName(), tabDatafile.getFileFormat(),
            tabDatafile.getContentType(),
            tabDatafile.getRowType(), tabDatafile.getType(), tabDatafile.getColumns(),
            tabDatafile.getRecordIdentifier(), tabDatafile.getDefaultValues(),
            lineOffset, withHeader, tabDatafile.getDelimiterChar(), -1,
            tabDatafile.getMetadataFolder(), Optional.of(tabDatafile));
  }

  /**
   * Prepare the {@link DataFile} for evaluation.
   * This step includes reading the headers from the file and generating a list of {@link TabularDataFile}.
   *
   *
   * @param dataFile
   * @param destinationFolder Preparing {@link DataFile} includes rewriting them in a normalized format. This {@link Path}
   *                          represents the destination of normalized files.
   * @return a list of {@link TabularDataFile}
   * @throws IOException
   * @throws IllegalStateException
   */
  public static DwcDataFile prepareDataFile(DataFile dataFile, Path destinationFolder) throws IOException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");
    Objects.requireNonNull(destinationFolder, "destinationFolder shall be provided");
    Preconditions.checkState(Files.isDirectory(destinationFolder), "destinationFolder should point to a folder");

    List<TabularDataFile> dataFileList = new ArrayList<>();
    switch (dataFile.getFileFormat()) {
      case SPREADSHEET:
        //FIXME if the contentType is not supported this will failed silently (except it will be log)
        handleSpreadsheetConversion(dataFile, destinationFolder).ifPresent(dataFileList::add);
        break;
      case DWCA:
        dataFileList.addAll(prepareDwcA(dataFile, destinationFolder));
        break;
      case TABULAR:
        //FIXME if the Optional is empty, it will fail silently
        prepareTabular(dataFile, destinationFolder).ifPresent(dataFileList::add);
    }

    Map<DwcFileType, List<TabularDataFile>> dfPerDwcFileType = dataFileList.stream()
            .collect(Collectors.groupingBy(TabularDataFile::getType));

    TabularDataFile coreDf = dfPerDwcFileType.get(DwcFileType.CORE).get(0);

    return new DwcDataFile(dataFile, coreDf,
            Optional.ofNullable(dfPerDwcFileType.get(DwcFileType.EXTENSION)));
  }

  /**
   * Given a {@link DataFile} pointing to folder containing the extracted DarwinCore archive, this method creates
   * a list of {@link TabularDataFile} for each of the data component (core + extensions).
   *
   * @param dwcaDataFile
   * @return
   */
  private static List<TabularDataFile> prepareDwcA(DataFile dwcaDataFile, Path destinationFolder) throws IOException {
    Validate.isTrue(dwcaDataFile.getFilePath().toFile().isDirectory(),
            "dwcaDataFile.getFilePath() must point to a directory");

    //ensure files are normalized
    Map<Path, Integer> normalizedFiles = FileNormalizer.normalizeFolderContent(dwcaDataFile.getFilePath(),
            destinationFolder, Optional.empty());

    List<TabularDataFile> dataFileList = new ArrayList<>();
    try (DwcReader dwcReader = new DwcReader(destinationFolder.toFile())) {
      //add the core first
      dataFileList.add(createDwcDataFile(dwcaDataFile, DwcFileType.CORE, dwcReader.getRowType(),
              dwcReader.getCore(), normalizedFiles.get(Paths.get(dwcReader.getCore().getLocation())) , destinationFolder));
      for (ArchiveFile ext : dwcReader.getExtensions()) {
        dataFileList.add(createDwcDataFile(dwcaDataFile, DwcFileType.EXTENSION, ext.getRowType(),
                ext, normalizedFiles.get(Paths.get(ext.getLocation())), destinationFolder));
      }
    }
    return dataFileList;
  }

  /**
   * Function responsible to transform a {@link DataFile} into a {@link TabularDataFile}.
   * File will be normalized and lines will be counted.
   *
   * @param tabularDataFile
   * @return
   * @throws IOException
   */
  private static Optional<TabularDataFile> prepareTabular(DataFile tabularDataFile, Path destinationFolder)
          throws IOException {

    Preconditions.checkArgument(FileFormat.TABULAR.equals(tabularDataFile.getFileFormat()), "prepareTabular can only" +
            "prepare FileFormat.TABULAR file");

    String fileExt = FilenameUtils.getExtension(tabularDataFile.getFilePath().getFileName().toString());
    Path destinationFile = destinationFolder.resolve(UUID.randomUUID() + fileExt);
    int numberOfLines = (int)FileNormalizer.normalizeFile(tabularDataFile.getFilePath(), destinationFile,
            Optional.empty());

    Character delimiter = getDelimiter(tabularDataFile.getFilePath());
    return Optional.of(buildTabularDataFile(destinationFile, tabularDataFile.getSourceFileName(),
            tabularDataFile.getContentType(), delimiter, numberOfLines, Optional.of(tabularDataFile)));
  }

  /**
   * Creates a new {@link TabularDataFile} representing a DarwinCore rowType.
   * @param dwcaDatafile
   * @param type
   * @param rowType
   * @param archiveFile
   * @return
   * @throws IOException
   */
  private static TabularDataFile createDwcDataFile(DataFile dwcaDatafile, DwcFileType type, Term rowType,
                                                   ArchiveFile archiveFile, int numberOfLines,
                                                   Path destinationFolder) throws IOException {

    Preconditions.checkArgument(dwcaDatafile.getFilePath().toFile().isDirectory(),
            "dwcaDatafile is expected to be a directory containing the Dwc-A files");

    Term[] headers;
    Optional<Map<Term, String>> defaultValues;
    Optional<TermIndex> recordIdentifier;

    //open DwcReader on specific dwcComponent (rowType)
    try (DwcReader rs = new DwcReader(dwcaDatafile.getFilePath().toFile(), Optional.of(rowType))) {
      headers = rs.getHeaders();
      defaultValues = rs.getDefaultValues();
      recordIdentifier = rs.getRecordIdentifier();
    }

    return new TabularDataFile(archiveFile.getLocationFile().toPath(),
            archiveFile.getLocationFile().getName(), FileFormat.DWCA,
            dwcaDatafile.getContentType(),
            rowType, type, headers, recordIdentifier, defaultValues,
            Optional.empty(), archiveFile.getIgnoreHeaderLines()!= null
            && archiveFile.getIgnoreHeaderLines() > 0, archiveFile.getFieldsTerminatedBy().charAt(0), numberOfLines,
            Optional.of(dwcaDatafile.getFilePath()),
            Optional.of(dwcaDatafile));
  }

  /**
   * Perform conversion of Spreadsheets.
   *
   * @param spreadsheetDataFile {@link DataFile} that requires conversion.
   * @param destinationFolder destination folder where the output will be stored in the form a random UUID with
   *                          CSV_EXT.
   *
   * @return new {@link TabularDataFile} instance representing the converted file or Optional.empty()
   * if the contentType can not be handled.
   *
   * @throws IOException
   */
  private static Optional<TabularDataFile> handleSpreadsheetConversion(DataFile spreadsheetDataFile, Path destinationFolder)
          throws IOException {

    Path spreadsheetFile = spreadsheetDataFile.getFilePath();
    String contentType = spreadsheetDataFile.getContentType();

    Path csvFile = destinationFolder.resolve(UUID.randomUUID() + CSV_EXT);
    int numberOfLine;
    if (ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET.equalsIgnoreCase(contentType) ||
            ExtraMediaTypes.APPLICATION_EXCEL.equalsIgnoreCase(contentType)) {
      numberOfLine = SpreadsheetConverters.convertExcelToCSV(spreadsheetFile, csvFile, SELECT_EXCEL_SHEET);
    } else if (ExtraMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET.equalsIgnoreCase(contentType)) {
      numberOfLine = SpreadsheetConverters.convertOdsToCSV(spreadsheetFile, csvFile);
    } else {
      LOG.warn("Unhandled contentType {}", contentType);
      return Optional.empty();
    }

    char delimiter = ',';
    return Optional.of(buildTabularDataFile(csvFile, spreadsheetDataFile.getSourceFileName(),
            ExtraMediaTypes.TEXT_CSV, delimiter, numberOfLine, Optional.of(spreadsheetDataFile)));

  }

  /**
   * Build a {@link TabularDataFile} for tabular files (csv, csv from converted Excel sheet).
   * The file is assumed to be already normalized
   *
   * @param tabularFilePath {@link Path} to the tabular file
   * @param sourceFileName
   * @param contentType
   * @param delimiter
   * @param numberOfLine
   * @param parentFile
   * @return
   */
  private static TabularDataFile buildTabularDataFile(Path tabularFilePath, String sourceFileName,
                                                      String contentType, Character delimiter, int numberOfLine,
                                                      Optional<DataFile> parentFile) throws IOException {
    Term[] headers;
    Optional<Term> rowType;
    Optional<TermIndex> recordIdentifier;
    try (RecordSource rs = RecordSourceFactory.fromDelimited(tabularFilePath.toFile(), delimiter, true)) {
      headers = rs.getHeaders();
      rowType = determineRowType(Arrays.asList(headers));
      recordIdentifier = determineRecordIdentifier(Arrays.asList(headers));
    }

    return new TabularDataFile(tabularFilePath,
            sourceFileName, FileFormat.TABULAR,
            contentType,
            rowType.orElse(null), DwcFileType.CORE, headers,
            recordIdentifier,
            Optional.empty(), //no default values
            Optional.empty(),  //no line offset
            true, delimiter, numberOfLine,
            Optional.empty(), //no metadata folder supported for tabular file at the moment
            parentFile);
  }

  /**
   * Tries to determine the rowType of a file based on its headers.
   *
   * @param headers
   *
   * @return
   */
  @VisibleForTesting
  protected static Optional<Term> determineRowType(List<Term> headers) {
    return TERM_TO_ROW_TYPE.entrySet().stream()
            .filter(ke -> headers.contains(ke.getKey()))
            .map(Map.Entry::getValue).findFirst();
  }

  /**
   * Tries to determine the record identifier of a file based on its headers.
   *
   * @param headers the list can contain null value when a column is used but the Term is undefined
   *
   * @return
   */
  @VisibleForTesting
  protected static Optional<TermIndex> determineRecordIdentifier(List<Term> headers) {
    //try to find the first matching term respecting the order defined by ID_TERMS
    return ID_TERMS.stream()
            .filter(t -> headers.contains(t))
            .findFirst()
            .map(t -> new TermIndex(headers.indexOf(t), t));
  }

  /**
   * Guesses the delimiter character form the data file.
   *
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
