package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.collector.CollectorGroup;

/**
 * This class encapsulates the result of processing a file by an Akka actor.
 */
class DataWorkResult {

  /**
   * Represents the result status of processing a file.
   */
  public enum Result {
    SUCCESS, FAILED;
  }

  private DataFile dataFile;

  private CollectorGroup collectors;
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
  DataWorkResult(DataFile dataFile, Result result, CollectorGroup collectors) {
    this.dataFile = dataFile;
    this.result = result;
    this.collectors = collectors;
  }

  public CollectorGroup getCollectors() {
    return collectors;
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
