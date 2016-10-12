package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.utils.file.tabular.TabularFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

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
          throws FileNotFoundException {
    return new TabularFileReader(TabularFiles.newTabularFileReader(new FileInputStream(sourceFile), delimiterChar,
            headerIncluded));
  }

}
