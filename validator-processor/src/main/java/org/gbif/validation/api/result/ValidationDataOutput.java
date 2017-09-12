package org.gbif.validation.api.result;

/**
 *
 */
public class ValidationDataOutput {

  /**
   * Enumerates the possible data output of Job.
   */
  public enum Type {
    DATASET_OBJECT;
  }

  private Type type;
  private Object content;

  public ValidationDataOutput(Type type, Object content) {
    this.type = type;
    this.content = content;
  }

  public Type getType() {
    return type;
  }

  public Object getContent() {
    return content;
  }
}
