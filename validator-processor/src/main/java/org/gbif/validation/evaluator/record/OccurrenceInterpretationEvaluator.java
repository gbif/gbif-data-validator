package org.gbif.validation.evaluator.record;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TermIndex;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.util.OccurrenceToTermsHelper;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotNull;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.evaluator.InterpretationRemarkEvaluationTypeMapping.INTERPRETATION_REMARK_MAPPING;

/**
 * Class to evaluate an occurrence record using an {@link OccurrenceInterpreter}.
 */
@ThreadSafe
public class OccurrenceInterpretationEvaluator implements RecordEvaluator {

  private static final Term OCC_ROW_TYPE = DwcTerm.Occurrence;

  private final OccurrenceInterpreter interpreter;
  private final Term[] columnMapping;
  private final Map<Term, String> defaultValues;
  private final TermIndex recordIdentifier;

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceInterpretationEvaluator.class);

  private static final Predicate<OccurrenceIssue> IS_MAPPED = issue -> INTERPRETATION_REMARK_MAPPING.containsKey(issue);

  /**
   * Default constructor.
   *
   * @param interpreter occurrence interpreter
   * @param columnMapping indices based column mapping. Unmapped column are expected to be represented by null
   * @param defaultValues
   * @param recordIdentifier
   */
  public OccurrenceInterpretationEvaluator(OccurrenceInterpreter interpreter,
                                           Term[] columnMapping, Map<Term, String> defaultValues,
                                           TermIndex recordIdentifier) {
    Validate.notNull(interpreter, "OccurrenceInterpreter must not be null");
    Validate.notNull(columnMapping, "columnMapping must not be null");

    this.interpreter = interpreter;
    this.columnMapping = columnMapping;
    this.defaultValues = defaultValues;
    this.recordIdentifier = recordIdentifier;
  }

  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable List<String> record) {
    LOG.debug("Evaluating line {} and record {}", lineNumber, record);
    if (record == null || record.isEmpty()) {
      return null;
    }

    VerbatimOccurrence verbatimOccurrence = toVerbatimOccurrence(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult(lineNumber, interpreter.interpret(verbatimOccurrence, null));
  }

  /**
   * Build a VerbatimOccurrence from a record represented as an array of values (as String).
   * Values indices shall match the column mapping of this evaluator.
   * @param record
   * @return new VerbatimOccurrence, never null
   */
  @VisibleForTesting
  protected VerbatimOccurrence toVerbatimOccurrence(@NotNull List<String> record) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    IntStream.range(0, Math.min(record.size(), columnMapping.length))
            .filter(idx -> columnMapping[idx] != null)
            .forEach(i -> verbatimOccurrence.setVerbatimField(columnMapping[i], record.get(i)));

    //only set a default value if the field is currently empty (this matches the crawler behavior)
    if (defaultValues != null) {
      defaultValues.forEach((k, v) -> {
        if (StringUtils.isBlank(verbatimOccurrence.getVerbatimField(k))) {
          verbatimOccurrence.setVerbatimField(k, v);
        }
      });
    }
    return verbatimOccurrence;
  }

  /**
   * Creates a RecordEvaluationResult from an OccurrenceInterpretationResult.
   * Responsible to put the related data (e.g. field + current value) into the RecordEvaluationResult instance.
   * @param lineNumber
   * @param result
   * @return
   */
  @VisibleForTesting
  protected RecordEvaluationResult toEvaluationResult(Long lineNumber, OccurrenceInterpretationResult result) {
    LOG.debug("Interpretation result original {} result {}", result.getOriginal(), result.getUpdated());
    RecordEvaluationResult.Builder builder = RecordEvaluationResult.Builder.of(OCC_ROW_TYPE, lineNumber,
            recordIdentifier == null ? null : result.getOriginal().getVerbatimField(recordIdentifier.getTerm()));

    Map<Term, String> verbatimFields = result.getUpdated().getVerbatimFields();
    builder.withInterpretedData(OccurrenceToTermsHelper.getTermsMap(result.getUpdated()));

    result.getUpdated().getIssues().stream()
            .filter(IS_MAPPED)
            .forEach(issue -> {
              Map<Term, String> relatedData = issue.getRelatedTerms()
                      .stream()
                      .filter(t -> verbatimFields.get(t) != null)
                      .collect(Collectors.toMap(Function.identity(), verbatimFields::get));
              builder.addInterpretationDetail(INTERPRETATION_REMARK_MAPPING.get(issue),
                      relatedData);

            });
    return builder.build();
  }

}
