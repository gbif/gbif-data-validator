package org.gbif.validation.api.result;

import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

import static org.gbif.validation.api.model.EvaluationCategory.RESOURCE_STRUCTURE;

/**
 * Factory providing {@link ValidationIssue} implementations.
 */
public class ValidationIssues {

  /**
   * Static factory class
   */
  private ValidationIssues(){}

  /**
   * Get a new instance of a {@link ValidationIssue} representing an exception in the validation.
   * An exception means a Java exception that was catch.
   * @param evaluationType
   * @param exception
   * @return
   */
  public static ValidationIssue withException(EvaluationType evaluationType, String exception){
    return new SampleBasedValidationIssue(evaluationType, 1l, exception);
  }

  /**
   * Get a new instance of a {@link ValidationIssue} representing a sampling of {@link ValidationResultDetails}.
   * @param evaluationType
   * @param count
   * @param sample
   * @return
   */
  public static ValidationIssue withSample(EvaluationType evaluationType, long count, List<ValidationResultDetails> sample){
    return new SampleBasedValidationIssue(evaluationType, count, sample);
  }

  /**
   * Get a new instance of a {@link ValidationIssue} representing a issue with related data on the resource structure.
   * Note that the scope of {@link ValidationIssue} is at rowType level and NOT line/record level.
   * Therefore, relatedData should respect that scope.
   * @param evaluationType
   * @param relatedData data related to the issue triggered at the rowType scope
   * @return
   */
  public static ValidationIssue withRelatedData(EvaluationType evaluationType, String relatedData){
    Validate.validState(RESOURCE_STRUCTURE.equals(evaluationType.getCategory()),
            "withRelatedData can only be used with EvaluationCategory %s", RESOURCE_STRUCTURE);
    return new ValidationIssueWithRelatedData(evaluationType, relatedData);
  }

  /**
   * Base class used internally.
   */
  private static abstract class ValidationIssueBase implements ValidationIssue, Serializable {
    protected final EvaluationType issue;
    protected ValidationIssueBase(EvaluationType issue){
      this.issue = issue;
    }

    @Override
    public EvaluationType getIssue() {
      return issue;
    }

    @Override
    public EvaluationCategory getIssueCategory() {
      if(issue == null) {
        return null;
      }
      return issue.getCategory();
    }
  }

  /**
   * Represents the output of a specific {@link EvaluationType}.
   *
   * Immutable class
   */
  private static class SampleBasedValidationIssue extends ValidationIssueBase {

    private final long count;
    private final List<ValidationResultDetails> sample;
    private final String exception;

    SampleBasedValidationIssue(EvaluationType issue, long count) {
      this(issue, count, null, null);
    }

    SampleBasedValidationIssue(EvaluationType issue, long count, String exception) {
      this(issue, count, null, exception);
    }

    SampleBasedValidationIssue(EvaluationType issue, long count, List<ValidationResultDetails> sample) {
      this(issue, count, sample, null);
    }

    SampleBasedValidationIssue(EvaluationType issue, long count, List<ValidationResultDetails> sample, String exception) {
      super(issue);
      this.count = count;
      this.sample = sample;
      this.exception = exception;
    }

    public long getCount() {
      return count;
    }

    @Nullable
    public List<ValidationResultDetails> getSample() {
      return sample;
    }

    public String getException() {
      return exception;
    }
  }

  private static class ValidationIssueWithRelatedData extends ValidationIssueBase {
    private final String relatedData;
    ValidationIssueWithRelatedData(EvaluationType issue, String relatedData) {
      super(issue);
      this.relatedData = relatedData;
    }

    public String getRelatedData() {
      return relatedData;
    }
  }

}
