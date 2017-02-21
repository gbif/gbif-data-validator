package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.ArchiveField;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.utils.file.csv.CSVReader;
import org.gbif.validation.api.RecordSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * {@link RecordSource} implementation for DarwinCore Archive.
 * This reader can work on the core file, an extension file or a portion of one of them (after splitting).
 *
 */
class DwcReader implements RecordSource {

  public static final Term DEFAULT_ID_TERM = TermFactory.instance().findTerm("ARCHIVE_RECORD_ID");

  private final Archive archive;

  //could be the core or an extension
  private final ArchiveFile darwinCoreComponent;
  private final List<ArchiveField> archiveFields;
  private final CSVReader csvReader;
  private final Term[] headers;

  private Optional<Map<Term, String>> defaultValues = Optional.empty();

  /**
   * Get a new Reader for the core component of the Dwc-A.
   *
   * @param dwcFolder
   * @throws IOException
   */
  DwcReader(File dwcFolder) throws IOException {
    this(dwcFolder, Optional.empty());
  }

  /**
   * Get a new Reader for an extension of the Dwc-A or the core if rowType is not provided.
   *
   * @param dwcFolder
   * @param rowType can be null to get the core
   * @throws IOException
   */
  DwcReader(File dwcFolder, Optional<Term> rowType) throws IOException {
    Objects.requireNonNull(dwcFolder, "dwcFolder shall be provided");
    archive = ArchiveFactory.openArchive(dwcFolder);

    darwinCoreComponent = (!rowType.isPresent() || archive.getCore().getRowType().equals(rowType.get()))?
                                                  archive.getCore() : archive.getExtension(rowType.get());

    archiveFields = darwinCoreComponent.getFieldsSorted();
    csvReader = darwinCoreComponent.getCSVReader();

    //check if there is default value(s) defined
    archiveFields.stream().filter(af -> af.getIndex() == null)
            .forEach(af -> addDefaultValue(af.getTerm(), af.getDefaultValue()));
    headers = extractHeaders();
  }

  /**
   * Get a new reader for the core or an extension that uses a portion of the original file obtained after splitting.
   *
   * @param dwcFolder
   * @param partFile portion of the original file obtained after splitting
   * @param rowType
   * @param ignoreHeaderLines
   * @throws IOException
   */
  DwcReader(File dwcFolder, File partFile, @Nullable Term rowType, boolean ignoreHeaderLines) throws IOException {
    Objects.requireNonNull(dwcFolder, "dwcFolder shall be provided");

    archive = ArchiveFactory.openArchive(dwcFolder);

    if (archive.getCore() == null) {
      throw new UnsupportedArchiveException("The archive must have a least a core file.");
    }

    if (rowType == null) {
      darwinCoreComponent = archive.getCore();
    } else {
      darwinCoreComponent = archive.getCore().getRowType().equals(rowType) ? archive.getCore() : archive.getExtension(rowType);
    }

    //TODO if darwinCoreComponent is null ?
    archiveFields = darwinCoreComponent.getFieldsSorted();
    csvReader =  new CSVReader(partFile, darwinCoreComponent.getEncoding(),
            darwinCoreComponent.getFieldsTerminatedBy(), darwinCoreComponent.getFieldsEnclosedBy(), ignoreHeaderLines ? 1 : 0);

    //check if there is default value(s) defined
    archiveFields.stream().filter(af -> af.getIndex() == null)
            .forEach(af -> addDefaultValue(af.getTerm(), af.getDefaultValue()));

    headers = extractHeaders();
  }

  public ArchiveFile getCore() {
    return archive.getCore();
  }

  /**
   * Get a Set of the extensions registered in this archive.
   *
   * @return never null
   */
  public Set<ArchiveFile> getExtensions(){
    return archive.getExtensions();
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    return headers;
  }


  /**
   * The purpose is this method is to extract headers from the DarwinCore Archive.
   * The size of the array will be determined by the maximum value of "index" in the definition of the archive.
   * If the "id" is not mapped to any Term, the term DEFAULT_ID_TERM will be assigned to it.
   *
   * @return
   * @throws UnsupportedArchiveException
   */
  private Term[] extractHeaders() throws UnsupportedArchiveException {
    if (archiveFields == null) {
      return null;
    }

    List<ArchiveField> archiveFieldsWithIndex = archiveFields.stream().filter(af -> af.getIndex() != null)
            .collect(Collectors.toList());

    //we assume the id is provided (it is mandatory by the schema)
    Integer idIndex = darwinCoreComponent.getId().getIndex();

    int maxIndex = archiveFieldsWithIndex.stream()
            .mapToInt(ArchiveField::getIndex).max().getAsInt();
    maxIndex = Math.max(maxIndex, darwinCoreComponent.getId().getIndex());

    Term[] terms = new Term[maxIndex + 1];
    // handle id column, assign default Term, it will be rewritten below if assigned to a term
    terms[idIndex] = DEFAULT_ID_TERM;
    archiveFieldsWithIndex.stream().forEach(af -> terms[af.getIndex()] = af.getTerm());
    return terms;
  }

  private void addDefaultValue(Term term, String value){
    if(!defaultValues.isPresent()){
      defaultValues = Optional.of(new HashMap<>());
    }
    defaultValues.get().put(term, value);
  }

  @Override
  public Optional<Map<Term, String>> getDefaultValues() {
    return defaultValues;
  }

  @Override
  public String[] read() throws IOException {
      return csvReader.next();
  }

  public Term getRowType() {
    if (darwinCoreComponent == null) {
      return null;
    }
    return darwinCoreComponent.getRowType();
  }

  /**
   * Get the {@link Term} representing the "id" of the current rowType.
   *
   * @return
   */
  public Optional<Term> getRecordIdentifier() {
    if (darwinCoreComponent == null || darwinCoreComponent.getId() == null) {
      return Optional.empty();
    }

    return Optional.of(headers[darwinCoreComponent.getId().getIndex()]);

  }

  @Override
  public void close() throws IOException {
    csvReader.close();
  }
}
