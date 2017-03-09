package org.gbif.validation.api.model;

/**
 * Data file format.
 */
public enum FileFormat {
  DWCA(true),
  TABULAR(true),
  SPREADSHEET(false);

  boolean tabularBased;

  FileFormat(boolean tabularBased) {
    this.tabularBased = tabularBased;
  }

  public boolean isTabularBased() {
    return tabularBased;
  }
}
