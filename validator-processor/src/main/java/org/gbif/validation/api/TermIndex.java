package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;

/**
 * Simple holder for a Term and its related index.
 */
public class TermIndex {
  private int index;
  private Term term;

  public TermIndex(int index, Term term){
    this.index = index;
    this.term = term;
  }

  public int getIndex() {
    return index;
  }

  public Term getTerm() {
    return term;
  }
}