package org.gbif.validation.api.model;

import org.gbif.dwc.terms.Term;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of an evaluation of a single record produced by RecordEvaluator.
 * This is the view of the result for a single line/record with possibly multiple EvaluationType via RecordEvaluationResultDetails.
 *
 * Immutable once built.
 */
public class RecordEvaluationResult implements Serializable {

  //2 fields used to uniquely identify an element in the validation
  private final Term rowType;
  private final Long lineNumber;
  private final String recordId;

  private final Map<Term, Object> interpretedData;
  private final Map<Term, Object> verbatimData;
  private final List<RecordEvaluationResultDetails> details;

  /**
   * Use {@link Builder} to get an instance.
   * @param rowType
   * @param lineNumber
   * @param details
   * @param interpretedData
   */
  public RecordEvaluationResult(Term rowType, Long lineNumber, String recordId, List<RecordEvaluationResultDetails> details, Map<Term, Object> interpretedData,
                                Map<Term, Object> verbatimData) {
    this.lineNumber = lineNumber;
    this.recordId = recordId;
    this.rowType = rowType;
    this.details = details;
    this.interpretedData = interpretedData;
    this.verbatimData = verbatimData;
  }

  public Long getLineNumber() {
    return lineNumber;
  }

  public String getRecordId() {
    return recordId;
  }

  public Term getRowType() {
    return rowType;
  }

  public List<RecordEvaluationResultDetails> getDetails(){
    return details;
  }

  /**
   * @return interpretedData or null
   */
  public Map<Term, Object> getInterpretedData() {
    return interpretedData;
  }

  /**
   * @return verbatimData or null
   */
  public Map<Term, Object> getVerbatimData() {
    return verbatimData;
  }

  @Override
  public String toString() {
    return "recordId: " + recordId + ", lineNumber: " + lineNumber +
            ", details: " + details +
            ", interpretedData " + interpretedData +
            ", verbatimData " + verbatimData;
  }

  /**
   * Builder class to build a RecordEvaluationResult instance.
   */
  public static class Builder {
    private Term rowType;
    private Long lineNumber;
    private String recordId;
    private Map<Term, Object> interpretedData;
    private Map<Term, Object> verbatimData;
    private List<RecordEvaluationResultDetails> details;

    public static Builder of(Term rowType, Long lineNumber, String recordId){
      return new Builder(rowType, lineNumber, recordId);
    }

    /**
     * rowType + lineNumber is used to uniquely identify a line within the validation routine.
     *
     * @param rowType
     * @param lineNumber
     * @return
     */
    public static Builder of(Term rowType, Long lineNumber){
      return of(rowType, lineNumber, null);
    }

    /**
     * rowType + recordId is used to uniquely identify a line within the validation routine.
     *
     * @param rowType
     * @param recordId
     * @return
     */
    public static Builder of(Term rowType, String recordId){
      return of(rowType, null, recordId);
    }

    private Builder(Term rowType, Long lineNumber, String recordId){
      this.rowType = rowType;
      this.lineNumber = lineNumber;
      this.recordId = recordId;
    }

    /**
     * Merge 2 @{link RecordEvaluationResult} into a single one.
     * The @{link RecordEvaluationResult} are assumed to come from the same record therefore no validation is performed
     * on the rowType and lineNumber and those from rer1 will be used.
     * If there is a collision on keys on the interpretedData map the values from rer2 will be in the
     * resulting object.
     * @param rer1
     * @param rer2
     * @return
     */
    public static RecordEvaluationResult merge(RecordEvaluationResult rer1, RecordEvaluationResult rer2) {
      // if something is null, try to return the other one, if both null, null is returned
      if(rer1 == null || rer2 == null) {
        return rer1 == null ? rer2 : rer1;
      }
      return new Builder(rer1.rowType, rer1.lineNumber, rer1.recordId)
              .fromExisting(rer1)
              .addDetails(rer2.getDetails())
              .putAllInterpretedData(rer2.getInterpretedData())
              .putAllVerbatimData(rer2.getVerbatimData())
              .build();
    }

    public Builder withInterpretedData(Map<Term, Object> interpretedData) {
      this.interpretedData = interpretedData;
      return this;
    }

    public Builder withVerbatimData(Map<Term, Object> verbatimData) {
      this.verbatimData = verbatimData;
      return this;
    }

    /**
     * Internal operation to copy the inner data structures from an existing {@link RecordEvaluationResult}
     *
     * @param recordEvaluationResult
     *
     * @return
     */
    private Builder fromExisting(RecordEvaluationResult recordEvaluationResult) {
      if (recordEvaluationResult.getDetails() != null) {
        details = new ArrayList<>(recordEvaluationResult.getDetails());
      }
      if (recordEvaluationResult.getInterpretedData() != null) {
        interpretedData = new HashMap<>(recordEvaluationResult.getInterpretedData());
      }
      if (recordEvaluationResult.getVerbatimData() != null) {
        verbatimData = new HashMap<>(recordEvaluationResult.getVerbatimData());
      }
      return this;
    }

    /**
     * Internal operation to copy details.
     *
     * @param details
     * @return
     */
    private Builder addDetails(List<RecordEvaluationResultDetails> details) {
      if(details == null){
        return this;
      }

      if(this.details == null){
        this.details = new ArrayList<>();
      }
      this.details.addAll(details);
      return this;
    }

    /**
     * Internal operation to copy interpretedData.
     *
     * @param interpretedData
     * @return
     */
    private Builder putAllInterpretedData(Map<Term, Object> interpretedData) {
      if(interpretedData == null){
        return this;
      }
      if(this.interpretedData == null){
        this.interpretedData = new HashMap<>();
      }
      this.interpretedData.putAll(interpretedData);
      return this;
    }

    /**
     * Internal operation to copy verbatimData.
     *
     * @param verbatimData
     * @return
     */
    private Builder putAllVerbatimData(Map<Term, Object> verbatimData) {
      if(verbatimData == null){
        return this;
      }
      if(this.verbatimData == null){
        this.verbatimData = new HashMap<>();
      }
      this.verbatimData.putAll(verbatimData);
      return this;
    }

    public Builder addInterpretationDetail(EvaluationType issueFlag, Map<Term, String> relatedData) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new RecordEvaluationResultDetails(issueFlag, relatedData));
      return this;
    }

    /**
     * Base detail is in the form of expected:found with an optional message but ideally we should be able to
     *
     * @param evaluationType
     * @param expected
     * @param found
     * @return
     */
    public Builder addBaseDetail(EvaluationType evaluationType, String expected, String found) {
      if(details == null){
        details = new ArrayList<>();
      }
      details.add(new RecordEvaluationResultDetails(evaluationType, expected, found));
      return this;
    }

    public RecordEvaluationResult build(){
      return new RecordEvaluationResult(rowType, lineNumber, recordId,
              details == null ? new ArrayList<>() : details, interpretedData, verbatimData);
    }
  }
}
