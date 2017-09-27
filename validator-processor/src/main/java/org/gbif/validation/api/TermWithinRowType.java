package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

/**
 * Represents a {@link Term} within the context of a row type.
 * e.g. “rowType”:”http://rs.tdwg.org/dwc/terms/Taxon”, ”term”:”http://rs.tdwg.org/dwc/terms/taxonID”
 */
public class TermWithinRowType {

  private Term rowType;
  private Term term;

  public static TermWithinRowType of(Term rowType, Term term) {
    return new TermWithinRowType(rowType, term);
  }

  /**
   * Row type alone, not term.
   * @param rowType
   * @return
   */
  public static TermWithinRowType ofRowType(Term rowType) {
    return new TermWithinRowType(rowType, null);
  }

  private TermWithinRowType(Term rowType, Term term) {
    this.rowType = rowType;
    this.term = term;
  }

  public Term getRowType() {
    return rowType;
  }

  public Term getTerm() {
    return term;
  }
}