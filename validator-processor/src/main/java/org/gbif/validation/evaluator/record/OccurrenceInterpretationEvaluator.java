package org.gbif.validation.evaluator.record;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.util.OccurrenceToTermsHelper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition.REMARKS_MAP;
import static org.gbif.validation.evaluator.InterpretationRemarkEvaluationTypeMapping.INTERPRETATION_REMARK_MAPPING;

/**
 * Class to evaluate an occurrence record using an {@link OccurrenceInterpreter}.
 */
public class OccurrenceInterpretationEvaluator implements RecordEvaluator {

  private final OccurrenceInterpreter interpreter;
  private final Term rowType;
  private final Term[] columnMapping;
  private final Optional<Map<Term, String>> defaultValues;

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceInterpretationEvaluator.class);

  private static final Predicate<OccurrenceIssue> IS_MAPPED = issue -> REMARKS_MAP.containsKey(issue)
                                                                       && INTERPRETATION_REMARK_MAPPING.containsKey(issue);

  /**
   * Default constructor, builds an instance using a OccurrenceInterpreter class.
   *
   * @param interpreter occurrence interpreter
   * @param columnMapping
   */
  public OccurrenceInterpretationEvaluator(OccurrenceInterpreter interpreter, Term rowType,
                                           List<Term> columnMapping, Optional<Map<Term, String>> defaultValues ) {
    Validate.notNull(interpreter, "OccurrenceInterpreter must not be null");
    Validate.notNull(columnMapping, "columnMapping must not be null");

    this.interpreter = interpreter;
    this.rowType = rowType;
    this.columnMapping = columnMapping.toArray(new Term[columnMapping.size()]);
    this.defaultValues = defaultValues;
  }

  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {
    LOG.info("Evaluating line {} and record {}", lineNumber, record);
    if (record == null || record.length == 0) {
      return null;
    }

    VerbatimOccurrence verbatimOccurrence = toVerbatimOccurrence(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult(lineNumber, interpreter.interpret(verbatimOccurrence));
  }

  /**
   * -- Visible For Testing --
   * Build a VerbatimOccurrence from a record represented as an array of values (as String).
   * Values indices shall match the column mapping of this evaluator.
   * @param record
   * @return new VerbatimOccurrence, never null
   */
  protected VerbatimOccurrence toVerbatimOccurrence(@NotNull String[] record) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    IntStream.range(0, Math.min(record.length, columnMapping.length))
      .forEach(i -> verbatimOccurrence.setVerbatimField(columnMapping[i], record[i]));

    defaultValues.ifPresent(map -> map.forEach( (k,v) -> verbatimOccurrence.setVerbatimField(k,v)));
    return verbatimOccurrence;
  }

  /**
   * -- Visible For Testing --
   * Creates a RecordEvaluationResult from an OccurrenceInterpretationResult.
   * Responsible to put the related data (e.g. field + current value) into the RecordEvaluationResult instance.
   * @param lineNumber
   * @param result
   * @return
   */
  protected RecordEvaluationResult toEvaluationResult(Long lineNumber, OccurrenceInterpretationResult result) {

    LOG.info("Interpretation result original {} result {}", result.getOriginal(), result.getUpdated());
    RecordEvaluationResult.Builder builder = RecordEvaluationResult.Builder.of(rowType, lineNumber);

    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();
    builder.withInterpretedData(OccurrenceToTermsHelper.getTermsMap(result.getUpdated()));

    result.getUpdated().getIssues().stream().filter(IS_MAPPED).
      forEach(issue -> {

        Map<Term, String> relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue)
                .stream()
                .filter(t -> verbatimFields.get(t) != null)
                .collect(Collectors.toMap(Function.identity(), verbatimFields::get));
        builder.addInterpretationDetail(INTERPRETATION_REMARK_MAPPING.get(issue),
                relatedData);

      });
    return builder.build();
  }

}
