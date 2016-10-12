package org.gbif.occurrence.validation.tabular;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.RecordSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import static org.gbif.tabular.TermTabularFiles.newTermMappedTabularFileReader;

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
  public static RecordSource fromDelimited(File sourceFile, char delimiterChar,
                                           boolean headerIncluded, Term...columnMapping) throws FileNotFoundException {
    return new TabularFileReader(newTermMappedTabularFileReader(new FileInputStream(sourceFile), delimiterChar,
                                                                headerIncluded, columnMapping));
  }

}
