package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Basically wrapping all the components of DarwinCore based data.
 * It represents a DarwinCore file in the sense that data is related to Darwin Core terms.
 * The content of a {@link DwcDataFile} is expected to be standardized.
 */
public class DwcDataFile {

  private DataFile dataFile;

  //currently organized as "star schema"
  private TabularDataFile core;
  private Optional<List<TabularDataFile>> extensions;
  private final Map<Term, TabularDataFile> tabularDataFileByTerm;

  public DwcDataFile(DataFile dataFile, TabularDataFile core, Optional<List<TabularDataFile>> extensions) {
    this.dataFile = dataFile;
    this.core = core;
    this.extensions = extensions;

    Map<Term, TabularDataFile> fileByRowType = new HashMap<>();
    fileByRowType.put(core.getRowType(), core);

    extensions.ifPresent( ext -> ext.stream().forEach( e -> fileByRowType.put(e.getRowType(), e)));

    tabularDataFileByTerm = Collections.unmodifiableMap(fileByRowType);
  }

  public DataFile getDataFile() {
    return dataFile;
  }

  public List<TabularDataFile> getTabularDataFiles() {
    return new ArrayList(tabularDataFileByTerm.values());
  }

  /**
   *
   * @param term
   * @return matching {@link TabularDataFile} or null
   */
  public TabularDataFile getByRowType(Term term) {
    return tabularDataFileByTerm.get(term);
  }

  public TabularDataFile getCore() {
    return core;
  }

  public Optional<List<TabularDataFile>> getExtensions() {
    return extensions;
  }

}
