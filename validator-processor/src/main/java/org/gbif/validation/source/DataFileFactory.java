package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.util.FileNormalizer;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.source.TabularFileMetadataExtractor.*;


/**
 * Main factory used to create and prepare {@link DataFile} and {@link TabularDataFile}.
 * Those objects are not simple to create so we centralize this process here.
 */
public class DataFileFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DataFileFactory.class);

  private static final String CSV_EXT = ".csv";

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
            lineOffset, withHeader, tabDatafile.getCharacterEncoding(), tabDatafile.getDelimiterChar(),
            tabDatafile.getQuoteChar(), -1,
            tabDatafile.getMetadataFolder());
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
  public static DwcDataFile prepareDataFile(DataFile dataFile, Path destinationFolder) throws IOException,
          UnsupportedDataFileException {
    Objects.requireNonNull(dataFile.getFilePath(), "filePath shall be provided");
    Objects.requireNonNull(dataFile.getFileFormat(), "fileFormat shall be provided");
    Objects.requireNonNull(destinationFolder, "destinationFolder shall be provided");
    Preconditions.checkState(Files.isDirectory(destinationFolder), "destinationFolder should point to a folder");

    List<TabularDataFile> dataFileList = new ArrayList<>();
    switch (dataFile.getFileFormat()) {
      case SPREADSHEET:
        handleSpreadsheetConversion(dataFile, destinationFolder).ifPresent(dataFileList::add);
        break;
      case DWCA:
        dataFileList.addAll(prepareDwcA(dataFile, destinationFolder));
        break;
      case TABULAR:
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
    //FIXME we should use the character encoding defined in the meta.xml
    Map<Path, Integer> normalizedFiles = FileNormalizer.normalizeFolderContent(dwcaDataFile.getFilePath(),
            destinationFolder, Optional.empty());

    List<TabularDataFile> dataFileList = new ArrayList<>();
    DwcaMetadataExtractor dwcReader = new DwcaMetadataExtractor(destinationFolder.toFile());
    //add the core first
    dataFileList.add(createDwcDataFile(dwcaDataFile, DwcFileType.CORE, dwcReader.getRowType(),
            dwcReader.getCore(), normalizedFiles.get(Paths.get(dwcReader.getCore().getLocation())) , destinationFolder));
    for (ArchiveFile ext : dwcReader.getExtensions()) {
      dataFileList.add(createDwcDataFile(dwcaDataFile, DwcFileType.EXTENSION, ext.getRowType(),
              ext, normalizedFiles.get(Paths.get(ext.getLocation())), destinationFolder));
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
          throws IOException, UnsupportedDataFileException {

    Preconditions.checkArgument(FileFormat.TABULAR.equals(tabularDataFile.getFileFormat()), "prepareTabular can only" +
            "prepare FileFormat.TABULAR file");

    Charset charset = StandardCharsets.UTF_8;
    String fileExt = FilenameUtils.getExtension(tabularDataFile.getFilePath().getFileName().toString());
    Path destinationFile = destinationFolder.resolve(UUID.randomUUID() + fileExt);
    int numberOfLines = FileNormalizer.normalizeFile(tabularDataFile.getFilePath(), destinationFile,
            Optional.empty());
    try{
      TabularMetadata tabularMetadata = getTabularMetadata(tabularDataFile.getFilePath(), charset);
      return Optional.of(buildTabularDataFile(destinationFile, tabularDataFile.getSourceFileName(),
              tabularDataFile.getContentType(), charset, tabularMetadata.getDelimiterChar(),
              tabularMetadata.getQuoteChar(), numberOfLines));
    }
    catch (UnkownDelimitersException udEx) {
      //re-throw the exception as UnsupportedDataFileException
      throw new UnsupportedDataFileException(udEx.getMessage());
    }
  }

  /**
   * Creates a new {@link TabularDataFile} representing a DarwinCore rowType.
   *
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

    //open DwcaMetadataExtractor on specific dwcComponent (rowType)
    DwcaMetadataExtractor dmex = new DwcaMetadataExtractor(dwcaDatafile.getFilePath().toFile(), Optional.of(rowType));
    headers = dmex.getHeaders();
    defaultValues = dmex.getDefaultValues();
    recordIdentifier = dmex.getRecordIdentifier();

    return new TabularDataFile(archiveFile.getLocationFile().toPath(),
            archiveFile.getLocationFile().getName(), FileFormat.DWCA,
            dwcaDatafile.getContentType(),
            rowType, type, headers, recordIdentifier, defaultValues,
            Optional.empty(), archiveFile.getIgnoreHeaderLines()!= null
            && archiveFile.getIgnoreHeaderLines() > 0,
            Charset.forName(archiveFile.getEncoding()),
            archiveFile.getFieldsTerminatedBy().charAt(0),
            archiveFile.getFieldsEnclosedBy(), numberOfLines,
            Optional.of(dwcaDatafile.getFilePath()));
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
          throws IOException, UnsupportedDataFileException {

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
      throw new UnsupportedDataFileException(contentType + " can not be converted");
    }

    return Optional.of(buildTabularDataFile(csvFile, spreadsheetDataFile.getSourceFileName(),
            ExtraMediaTypes.TEXT_CSV, StandardCharsets.UTF_8, SpreadsheetConverters.DELIMITER_CHAR,
            SpreadsheetConverters.QUOTE_CHAR, numberOfLine));
  }

  /**
   * Build a {@link TabularDataFile} for tabular files (csv, csv from converted Excel sheet).
   * The file is assumed to be already normalized
   *
   * @param tabularFilePath {@link Path} to the tabular file
   * @param sourceFileName
   * @param contentType
   * @param delimiter
   * @param quoteChar
   * @param numberOfLine
   * @return
   */
  private static TabularDataFile buildTabularDataFile(Path tabularFilePath, String sourceFileName,
                                                      String contentType, Charset charset, Character delimiter,
                                                      Character quoteChar,
                                                      int numberOfLine) throws IOException, UnsupportedDataFileException {

    Term[] headers = extractHeader(tabularFilePath, charset, delimiter, quoteChar)
            .orElseThrow(() -> new UnsupportedDataFileException("Can't extract header"));
    Optional<Term> rowType = determineRowType(Arrays.asList(headers));
    Optional<TermIndex> recordIdentifier = determineRecordIdentifier(Arrays.asList(headers));

    return new TabularDataFile(tabularFilePath,
            sourceFileName, FileFormat.TABULAR,
            contentType,
            rowType.orElse(null), DwcFileType.CORE, headers,
            recordIdentifier,
            Optional.empty(), //no default values
            Optional.empty(),  //no line offset
            true, StandardCharsets.UTF_8, delimiter, quoteChar, numberOfLine,
            Optional.empty()); //no metadata folder supported for tabular file at the moment
  }

}
