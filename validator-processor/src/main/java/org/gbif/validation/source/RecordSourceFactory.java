package org.gbif.validation.source;

import org.gbif.utils.file.tabular.TabularFiles;
import org.gbif.validation.api.RecordSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Creates instances of RecordSource class.
 */
public class RecordSourceFactory {

  /**
   * Private constructor.
   */
  private RecordSourceFactory() {
    //empty method
  }

  /**
   * Creates instances of RecordSource from character delimited files.
   */
  public static RecordSource fromDelimited(File sourceFile, char delimiterChar, boolean headerIncluded)
          throws IOException {
    return new TabularFileReader(TabularFiles.newTabularFileReader(new FileInputStream(sourceFile), delimiterChar,
            headerIncluded));
  }

  /**
   * Creates instances of RecordSource from a folder containing an extracted DarwinCore archive.
   */
  public static RecordSource fromDwcA(File sourceFolder)
          throws IOException {
    return new DwcReader(sourceFolder);
  }

}
