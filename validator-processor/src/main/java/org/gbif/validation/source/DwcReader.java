package org.gbif.validation.source;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.ArchiveField;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.utils.file.csv.CSVReader;
import org.gbif.validation.api.RecordSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Currently, we only read the core file.
 *
 */
public class DwcReader implements RecordSource {

  private static final Term DEFAULT_ID_TERM = TermFactory.instance().findTerm("ARCHIVE_RECORD_ID");

  //could be the core or an extension
  private final ArchiveFile darwinCoreComponent;
  private final List<ArchiveField> archiveFields;
  private final CSVReader csvReader;

  private Term[] defaultValuesTerm;
  private String[] defaultValues;

  /**
   * Get a new Reader for the core component of the Dwc-A.
   *
   * @param dwcFolder
   * @throws IOException
   */
  DwcReader(File dwcFolder) throws IOException {
    this(dwcFolder, null);
  }

  /**
   * Get a new Reader for an extension of the Dwc-A.
   *
   * @param dwcFolder
   * @param rowType can be null to get the core
   * @throws IOException
   */
  DwcReader(File dwcFolder, @Nullable Term rowType) throws IOException {
    Objects.requireNonNull(dwcFolder, "dwcFolder shall be provided");

    Archive archive = ArchiveFactory.openArchive(dwcFolder);
    darwinCoreComponent = Optional.ofNullable(rowType).isPresent() ? archive.getExtension(rowType) : archive.getCore();
    archiveFields = darwinCoreComponent.getFieldsSorted();
    csvReader = darwinCoreComponent.getCSVReader();
  }

  @Nullable
  @Override
  public Term[] getHeaders() {
    if(archiveFields == null) {
      return null;
    }

    List<ArchiveField> termsWithDefaultValues = new ArrayList<>();
    // handle id column
    Term idColumnTerm =  Optional.ofNullable(darwinCoreComponent.getId().getTerm()).orElse(DEFAULT_ID_TERM);

    List<Term> terms = new ArrayList<>(archiveFields.size());
    terms.add(darwinCoreComponent.getId().getIndex(), idColumnTerm);

    for(ArchiveField af : archiveFields) {
      if(af.getIndex() != null){
        terms.add(af.getIndex(), af.getTerm());
      }
      else{
        termsWithDefaultValues.add(af);
      }
    }

    //handle default values (if any)
    if(!termsWithDefaultValues.isEmpty()) {
      defaultValuesTerm = new Term[termsWithDefaultValues.size()];
      defaultValues = new String[termsWithDefaultValues.size()];
      for (int i = 0; i < termsWithDefaultValues.size(); i++) {
        terms.add(termsWithDefaultValues.get(i).getTerm());
        defaultValuesTerm[i] = termsWithDefaultValues.get(i).getTerm();
        defaultValues[i] = termsWithDefaultValues.get(i).getDefaultValue();
      }
    }

    return terms.toArray(new Term[terms.size()]);
  }

  @Override
  public String[] read() throws IOException {
    if(defaultValuesTerm == null) {
      return csvReader.next();
    }

    String[] line = csvReader.next();
    if (line != null) {
      line = Arrays.copyOf(line, line.length + defaultValuesTerm.length);
      System.arraycopy(defaultValues, 0, line, line.length - 1, defaultValuesTerm.length);
    }

    return line;
  }

  @Nullable
  @Override
  public Path getFileSource() {
    if(darwinCoreComponent == null) {
      return null;
    }
    return darwinCoreComponent.getLocationFile().toPath();
  }

  public Term getRowType() {
    if(darwinCoreComponent == null) {
      return null;
    }
    return darwinCoreComponent.getRowType();
  }

  @Override
  public void close() throws IOException {
    csvReader.close();
  }
}
