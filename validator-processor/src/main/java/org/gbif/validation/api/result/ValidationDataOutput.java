package org.gbif.validation.api.result;


import java.util.Optional;

/**
 *
 */
public class ValidationDataOutput {

  /**
   * Enumerates the possible data output of Job.
   */
  public enum Type {
    DATASET_OBJECT;

    public static Optional<Type> fromString(String str) {
      for(Type t : values()) {
        if(t.name().equalsIgnoreCase(str)){
          return Optional.of(t);
        }
      }
      return Optional.empty();
    }
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
