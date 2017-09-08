package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Basically grouping all the components of DarwinCore based data.
 * It represents a DarwinCore file in the sense that data is related to Darwin Core terms.
 */
public class DwcDataFile {

  private final DataFile dataFile;

  //currently organized as "star schema"
  private final TabularDataFile core;
  private final List<TabularDataFile> extensions;
  private final Map<RowTypeKey, TabularDataFile> tabularDataFileByTerm;
  private final Path metadataFilePath;

  public DwcDataFile(DataFile dataFile, TabularDataFile core, List<TabularDataFile> extensions,
                     Path metadataFilePath) {
    this.dataFile = dataFile;
    this.core = core;
    this.extensions = extensions;
    this.metadataFilePath = metadataFilePath;

    Map<RowTypeKey, TabularDataFile> fileByRowType = new HashMap<>();
    fileByRowType.put(core.getRowTypeKey(), core);
    getExtensions().ifPresent(ext -> ext.stream()
            .forEach(e -> fileByRowType.put(e.getRowTypeKey(), e)));

    tabularDataFileByTerm = Collections.unmodifiableMap(fileByRowType);
  }

  public DataFile getDataFile() {
    return dataFile;
  }

  public List<TabularDataFile> getTabularDataFiles() {
    return new ArrayList<>(tabularDataFileByTerm.values());
  }

  /**
   * Considering a core and at least one extension can share the same rowType, this function returns a list.
   * @param term
   * @return matching {@link TabularDataFile} or null
   */
  public List<TabularDataFile> getByRowType(Term term) {
    return tabularDataFileByTerm.entrySet().stream()
            .filter( e -> term == e.getKey().getRowType())
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
  }

  public TabularDataFile getByRowTypeKey(RowTypeKey rowTypeKey) {
    return tabularDataFileByTerm.get(rowTypeKey);
  }

  public TabularDataFile getCore() {
    return core;
  }

  public Optional<List<TabularDataFile>> getExtensions() {
    return  Optional.ofNullable(extensions);
  }

  public Optional<Path> getMetadataFilePath() {
    return Optional.ofNullable(metadataFilePath);
  }
}
