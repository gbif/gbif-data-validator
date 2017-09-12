package org.gbif.validation.api.vocabulary;

/**
 * File type in the context of DarwinCore.
 */
public enum DwcFileType {
  META_DESCRIPTOR(false), // meta.xml
  METADATA(false), //eml.xml
  CORE(true),
  EXTENSION(true);

  boolean dataBased;
  DwcFileType(boolean dataBased) {
    this.dataBased = dataBased;
  }

  /**
   * Indicates if a {@link DwcFileType} is data based or not.
   * Data based means related to data as opposed to related to metadata.
   * @return
   */
  public boolean isDataBased() {
    return dataBased;
  }
}
