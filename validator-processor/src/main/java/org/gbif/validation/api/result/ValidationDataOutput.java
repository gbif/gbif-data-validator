package org.gbif.validation.api.result;


import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.gbif.validation.api.result.ValidationDataOutput.Type.VERBATIM_RECORD_SAMPLE;

/**
 * Represents the data produced in the context of a validation that is not directly in linked with the validation result itself.
 */
public class ValidationDataOutput {

  /**
   * Enumerates the possible data output of Job.
   */
  public enum Type {
    DATASET_OBJECT, VERBATIM_RECORD_SAMPLE;

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

  public static ValidationDataOutput verbatimRecordSample(Term[] headers, Map<Long, List<String>> records) {
    return new ValidationDataOutput(VERBATIM_RECORD_SAMPLE, new VerbatimRecordSampleDataOutput(headers, records));
  }

  public Type getType() {
    return type;
  }

  public Object getContent() {
    return content;
  }

  public static class VerbatimRecordSampleDataOutput {
    private Term[] headers;
    private Map<Long, List<String>> records;

    public VerbatimRecordSampleDataOutput(Term[] headers, Map<Long, List<String>> records) {
      this.headers = headers;
      this.records = records;
    }

    public Term[] getHeaders() {
      return headers;
    }

    public Map<Long, List<String>> getRecords() {
      return records;
    }
  }
}
