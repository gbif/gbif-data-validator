package org.gbif.validation.processor;

import org.gbif.validation.api.RowTypeKey;
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

  private RowTypeKey rowTypeKey;
  private String fileName;

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
  DataWorkResult(RowTypeKey rowTypeKey, String fileName, Result result, CollectorGroup collectors) {
    //Objects.requireNonNull(rowTypeKey, "rowTypeKey shall be provided");
    this.rowTypeKey = rowTypeKey;
    this.fileName = fileName;
    this.result = result;
    this.collectors = collectors;
  }

  public CollectorGroup getCollectors() {
    return collectors;
  }

  public RowTypeKey getRowTypeKey() {
    return rowTypeKey;
  }

  public void setRowTypeKey(RowTypeKey rowTypeKey) {
    this.rowTypeKey = rowTypeKey;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
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
    return "Result: " + (result != null ? result.name() : "null") + ", FileName: " + fileName;
  }
}
