package org.gbif.validation.api.result;

import org.gbif.api.jackson.MapEntrySerde;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.EvaluationCategory;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.vocabulary.DwcFileType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * A {@link ValidationResultElement} represents an element in the resource in validation.
 * For DarwinCore Archive this could be the meta.xml, a core file or an extension file.
 *
 * Scope: part (per rowType) of the resource submitted.
 *
 */
public class ValidationResultElement implements Serializable {

  private final String fileName;
  private final List<ValidationIssue> issues;

  private final Long numberOfLines;
  private final Long numberOfLinesWithData;

  //TODO replace by RowTypeKey
  private final DwcFileType fileType;
  //From Dwc "class of data represented by each row"
  private final Term rowType;
  private final Term idTerm;

  private final List<Map.Entry<Term, Integer>> termsFrequency;
  private final Map<Term, Long> interpretedValueCounts;

  private final List<ValidationDataOutput.Type> availableDataOutput;

  //this field is not serialized
  private final List<ValidationDataOutput> dataOutput;

  /**
   * Get a new {@link ValidationResultElement} that represents an exception linked to a {@link EvaluationType}.
   *
   * @param fileName
   * @param evaluationType
   * @param exception
   *
   * @return
   */
  public static ValidationResultElement onException(String fileName, EvaluationType evaluationType, String exception){
    //EvaluationType evaluationType
    List<ValidationIssue> issues = new ArrayList<>();
    issues.add(ValidationIssues.withException(evaluationType, exception));
    return new ValidationResultElement(fileName, null, null, null, null, null, issues, null);
  }

  /**
   * Get a new {@link ValidationResultElement} for {@link DwcFileType#METADATA}.
   *
   * @param fileName
   * @param issues
   * @return
   */
  public static ValidationResultElement forMetadata(String fileName, List<ValidationIssue> issues,
                                                    List<ValidationDataOutput> dataOutput){
    return new ValidationResultElement(fileName, null, null, DwcFileType.METADATA, null, null, issues, dataOutput);
  }

  /**
   * Get a new {@link ValidationResultElement} for {@link DwcFileType#META_DESCRIPTOR}.
   *
   * @param fileName
   * @param issues
   * @return
   */
  public static ValidationResultElement forMetaDescriptor(String fileName, List<ValidationIssue> issues){
    return new ValidationResultElement(fileName, null, null, DwcFileType.META_DESCRIPTOR, null, null, issues, null);
  }

  /**
   * Given 2 collections of {@link ValidationResultElement}, for each element of source merge the issues into
   * the mergeInto collection if it contains a {@link ValidationResultElement} with the same filename. Otherwise,
   * the {@link ValidationResultElement} is added to the mergeInto collection.
   *
   * FIXME availableDataOutput will not be updated after merge. We should probably return a new Collection
   *
   * @param source
   * @param mergeInto
   */
  public static void mergeOnFilename(final Collection<ValidationResultElement> source, final Collection<ValidationResultElement> mergeInto) {
    source.forEach(
            vre -> {
              Optional<ValidationResultElement> currentElement = mergeInto.stream()
                      .filter(re -> vre.getFileName().equals(re.getFileName()))
                      .findFirst();
              if (currentElement.isPresent()) {
                currentElement.get().getIssues().addAll(vre.getIssues());
                currentElement.get().getAvailableDataOutput().addAll(vre.getAvailableDataOutput());
              } else {
                mergeInto.add(vre);
              }
            }
    );
  }

  public ValidationResultElement(String fileName, Long numberOfLines, Long numberOfLinesWithData, DwcFileType fileType,
                                 Term rowType, Term idTerm, List<ValidationIssue> issues, List<ValidationDataOutput> dataOutput){
    this(fileName, numberOfLines, numberOfLinesWithData, fileType, rowType, idTerm, issues, null, null, dataOutput);
  }

  /**
   *
   * @param fileName
   * @param numberOfLines total number of line, including the header line
   * @param fileType
   * @param rowType
   * @param issueCounter
   * @param issueSampling
   * @param termsFrequency ordered list of key/value pairs
   * @param interpretedValueCounts
   */
  public ValidationResultElement(String fileName, Long numberOfLines, Long numberOfLinesWithData, DwcFileType fileType, Term rowType, Term idTerm,
                                 Map<EvaluationType, Long> issueCounter,
                                 Map<EvaluationType, List<ValidationResultDetails>> issueSampling,
                                 List<Map.Entry<Term, Integer>> termsFrequency,
                                 Map<Term, Long> interpretedValueCounts, List<ValidationDataOutput> dataOutput) {
    this(fileName, numberOfLines, numberOfLinesWithData, fileType, rowType, idTerm, new ArrayList<>(), termsFrequency,
            interpretedValueCounts, dataOutput);

    if (issueCounter != null && issueSampling != null) {
      issueCounter.forEach(
              (k, v) ->
                      issues.add(ValidationIssues.withSample(k, v, issueSampling.get(k))));
    }
  }

  /**
   * Full constructor
   *
   * @param fileName
   * @param numberOfLines
   * @param fileType
   * @param rowType
   * @param issues
   * @param termsFrequency
   * @param interpretedValueCounts
   */
  public ValidationResultElement(String fileName, Long numberOfLines, Long numberOfLinesWithData,
                                 DwcFileType fileType, Term rowType, Term idTerm,
                                 List<ValidationIssue> issues,
                                 List<Map.Entry<Term, Integer>> termsFrequency,
                                 Map<Term, Long> interpretedValueCounts,
                                 List<ValidationDataOutput> dataOutput) {
    this.fileName = fileName;
    this.numberOfLines = numberOfLines;
    this.numberOfLinesWithData = numberOfLinesWithData;
    this.fileType = fileType;
    this.rowType = rowType;
    this.idTerm = idTerm;
    this.issues = issues;
    this.termsFrequency = termsFrequency;
    this.interpretedValueCounts = interpretedValueCounts;
    this.dataOutput = dataOutput;

    this.availableDataOutput = dataOutput == null ? Collections.emptyList() :
            dataOutput.stream()
                    .map(ValidationDataOutput::getType)
                    .collect(Collectors.toList());
  }


  public String getFileName() {
    return fileName;
  }

  public Long getNumberOfLines() {
    return numberOfLines;
  }

  public Long getNumberOfLinesWithData() {
    return numberOfLinesWithData;
  }

  public DwcFileType getFileType() {
    return fileType;
  }

  public Term getRowType() {
    return rowType;
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

  @JsonSerialize(contentUsing = MapEntrySerde.MapEntryJsonSerializer.class)
  @JsonDeserialize(contentUsing = MapEntrySerde.MapEntryJsonDeserializer.class)
  public List<Map.Entry<Term, Integer>> getTermsFrequency() {
    return termsFrequency;
  }

  public Map<Term, Long> getInterpretedValueCounts() {
    return interpretedValueCounts;
  }

  public Term getIdTerm() {
    return idTerm;
  }

  public List<ValidationDataOutput.Type> getAvailableDataOutput() {
    return availableDataOutput;
  }

  @JsonIgnore
  public List<ValidationDataOutput> getDataOutput() {
    return dataOutput;
  }

  /**
   * Check if the list of issue contains at least one issue of the provided {@link EvaluationCategory}.
   * @param evaluationCategory
   * @return
   */
  public boolean contains(EvaluationCategory evaluationCategory) {
    return contains(vi -> evaluationCategory.equals(vi.getIssue().getCategory()));
  }

  /**
   * Check if the list of issue contains at least one issue of the provided {@link EvaluationType}.
   * @param evaluationType
   * @return
   */
  public boolean contains(EvaluationType evaluationType) {
    return contains(vi -> evaluationType.equals(vi.getIssue()));
  }

  /**
   * Check if the list of issue contains at least one issue of the provided {@link Set} of {@link EvaluationType}.
   * @param evaluationTypes
   * @return
   */
  public boolean containsAny(Set<EvaluationType> evaluationTypes) {
    return contains(vi -> evaluationTypes.contains(vi.getIssue()));
  }

  private boolean contains(Predicate<ValidationIssue> predicate) {
    if (issues != null) {
      return issues.stream()
              .filter(predicate)
              .findAny().isPresent();
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("fileName", fileName)
            .add("fileType", fileType)
            .add("rowType", rowType)
            .add("issues", issues)
            .add("numberOfLines", numberOfLines)
            .add("numberOfLinesWithData", numberOfLinesWithData)
            .add("termsFrequency", termsFrequency)
            .add("interpretedValueCounts", interpretedValueCounts)
            .add("availableDataOutput", availableDataOutput)
            .toString();
  }

}
