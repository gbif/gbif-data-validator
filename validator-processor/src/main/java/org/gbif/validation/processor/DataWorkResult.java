package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;

/**
 * This class encapsulates the result of processing a file by an Akka actor.
 */
public class DataWorkResult {

  /**
   * Represents the result status of processing a file.
   */
  public enum Result {
    SUCCESS, FAILED;
  }

  private DataFile dataFile;

  private Result result;

  /**
   * Empty constructor.
   * Required by data serialization.
   */
  DataWorkResult() {
    //empty block
  }

  /**
   * Full constructor.
   * Builds an instance using a dataFile and a result.
   */
  DataWorkResult(DataFile dataFile, Result result) {
    this.dataFile = dataFile;
    this.result = result;
  }

  /**
   * Data file processed.
   */
  public DataFile getDataFile() {
    return dataFile;
  }

  public void setDataFile(DataFile dataFile) {
    this.dataFile = dataFile;
  }

  /**
   * Status result of processing a data file.
   */
  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  @Override
  public String toString() {
    return "Result: " + result.name() + " Datafile: " + dataFile.getFilePath();
  }
}
