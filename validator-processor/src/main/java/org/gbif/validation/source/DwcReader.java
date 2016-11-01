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
import javax.annotation.Nullable;

/**
 * Currently, we only read the core file.
 *
 */
public class DwcReader implements RecordSource {

  private static final Term DEFAULT_ID_TERM = TermFactory.instance().findTerm("ARCHIVE_RECORD_ID");

  //could be the core or an extension
  private ArchiveFile darwinCoreComponent;
  private List<ArchiveField> archiveFields;
  private CSVReader csvReader;
  private Term idColumnTerm;

  private Term[] defaultValuesTerm;
  private String[] defaultValues;

  DwcReader(File dwcFolder) throws IOException {
    Archive archive = ArchiveFactory.openArchive(dwcFolder);

    darwinCoreComponent = archive.getCore();
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
    idColumnTerm = darwinCoreComponent.getId().getTerm() != null ? darwinCoreComponent.getId().getTerm() : DEFAULT_ID_TERM;

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

    if(line == null){
      return null;
    }
    line = Arrays.copyOf(line, line.length + defaultValuesTerm.length);
    System.arraycopy(defaultValues, 0, line, line.length-1, defaultValuesTerm.length);

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

  @Override
  public void close() throws IOException {
    csvReader.close();
  }
}
