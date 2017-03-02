package org.gbif.validation.source;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.csv.CSVReader;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.util.TempTermsUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Adapter for extracting information about file in tabular format.
 */
class TabularFileMetadataExtractor {

  /**
   * Terms that can represent an identifier within a file
   */
  private static final List<Term> ID_TERMS = Collections.unmodifiableList(
          Arrays.asList(DwcTerm.eventID, DwcTerm.occurrenceID, DwcTerm.taxonID, DcTerm.identifier));

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

  private TabularFileMetadataExtractor() {
  }

  static Optional<Term[]> extractHeader(Path filePath, Charset characterEncoding,
                                               Character delimiterChar, Character quoteChar)
          throws IOException{
    Term[] header = null;
    try (CSVReader csvReader = new CSVReader(new FileInputStream(filePath.toFile()),
            characterEncoding.name(),
            delimiterChar.toString(),
            quoteChar, 1, 0)) {
      String[] headerStrings = csvReader.getHeader();
      if(headerStrings != null) {
        header = TempTermsUtils.buildTermMapping(headerStrings);
      }
    }
    catch (IllegalArgumentException iaEx){ /** do nothing, we will return empty Optional*/}
    return Optional.ofNullable(header);
  }

  /**
   * Tries to determine the rowType of a file based on its headers.
   *
   * @param headers
   *
   * @return
   */
  static Optional<Term> determineRowType(List<Term> headers) {
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
  static Optional<TermIndex> determineRecordIdentifier(List<Term> headers) {
    //try to find the first matching term respecting the order defined by ID_TERMS
    return ID_TERMS.stream()
            .filter(t -> headers.contains(t))
            .findFirst()
            .map(t -> new TermIndex(headers.indexOf(t), t));
  }

}
