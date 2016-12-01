package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;

/**
 * Simple class keeping a DataLine to simplify the usage with Akka.
 */
class DataLine {

  private Term rowType;
  private String[] line;

  public DataLine(Term rowType, String[] line) {
    this.rowType = rowType;
    this.line = line;
  }

  public Term getRowType() {
    return rowType;
  }

  public String[] getLine() {
    return line;
  }

}
