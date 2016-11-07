package org.gbif.validation.tabular.parallel;

/**
 * Package protected class to keep a DataLine to simplify the usage with Akka.
 */
class DataLine {

  private String[] line;

  public DataLine(String[] line) {
    this.line = line;
  }

  public String[] getLine() {
    return line;
  }

}
