package org.gbif.occurrence.validation.util;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

/**
 *
 */
public class TempTermsUtils {

  /**
   * Private constructor.
   */
  private TempTermsUtils() {
    //empty constructor
  }

  public static Term[] buildTermMapping(String[] columns) {

    TermFactory termFactory = TermFactory.instance();

    Term[] columnMapping = new Term[columns.length];
    for (int i = 0; i < columns.length; i++) {
      columnMapping[i] = termFactory.findTerm(columns[i]);
    }
    return columnMapping;
  }
}
