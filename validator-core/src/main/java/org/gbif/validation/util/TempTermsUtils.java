package org.gbif.validation.util;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

/**
 * Utility class to deal with GBIF/DwC/Dc terms.
 */
public final class TempTermsUtils {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  /**
   * Private constructor.
   */
  private TempTermsUtils() {
    //empty constructor
  }


  /**
   * Lookups columns terms from string names.
   *
   * @param terms
   * @return
   * @throws IllegalArgumentException
   */
  public static Term[] buildTermMapping(String[] terms) throws IllegalArgumentException {
    Term[] columnMapping = new Term[terms.length];
    for (int i = 0; i < terms.length; i++) {
      columnMapping[i] = TERM_FACTORY.findTerm(terms[i]);
    }
    return columnMapping;
  }
}
