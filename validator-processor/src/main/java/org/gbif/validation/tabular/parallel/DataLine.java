package org.gbif.validation.tabular.parallel;

/**
 * Simple class keeping a DataLine to simplify the usage with Akka.
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
