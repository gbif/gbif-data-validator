package org.gbif.occurrence.validation.evaluator;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.api.model.RecordEvaluationResult;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class to process one text line that represents a occurrence record.
 */
public class OccurrenceInterpretationEvaluator implements RecordEvaluator {

  private final OccurrenceInterpreter interpreter;
  private final String[] fields;
  private final Term[] columnMapping;

  /**
   * Default constructor, builds an instance using a OccurrenceInterpreter class.
   * @param interpreter occurrence interpreter
   */
  public OccurrenceInterpretationEvaluator(OccurrenceInterpreter interpreter, String[] fields, Term[] columnMapping) {
    this.interpreter = interpreter;
    this.fields = fields;
    this.columnMapping = columnMapping;
  }

  @Override
  public RecordEvaluationResult evaluate(@Nullable Long lineNumber, String[] record) {
    VerbatimOccurrence verbatimOccurrence = toVerbatimOccurrence(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult(lineNumber, interpreter.interpret(verbatimOccurrence));
  }

  private VerbatimOccurrence toVerbatimOccurrence(String[] record){
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    int numOfColumns = Math.min(record.length, columnMapping.length);

    for(int i = 0; i < numOfColumns; ++i) {
      verbatimOccurrence.setVerbatimField(columnMapping[i], record[i]);
    }
    return verbatimOccurrence;
  }

  @Override
  public String[] getFields() {
    return fields;
  }

  /**
   * Creates a RecordInterpretationResult from an OccurrenceInterpretationResult.
   * Responsible to to put the related data (e.g. field + current value) into the RecordInterpretionBasedEvaluationResult
   * instance.
   */
  private static RecordEvaluationResult toEvaluationResult(Long lineNumber, OccurrenceInterpretationResult result) {

    RecordEvaluationResult.Builder builder = new RecordEvaluationResult.Builder();
    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();

    builder.withLineNumber(lineNumber);
    result.getUpdated().getIssues().forEach( issue -> {
      if (InterpretationRemarksDefinition.REMARKS_MAP.containsKey(issue) &&
              OccurrenceIssueEvaluationTypeMapping.OCCURRENCE_ISSUE_MAPPING.containsKey(issue)) {
        Map<Term, String> relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue)
          .stream()
          .filter(t -> verbatimFields.get(t) != null)
          .collect(Collectors.toMap(Function.identity(), verbatimFields::get));
        builder.addInterpretationDetail(OccurrenceIssueEvaluationTypeMapping.OCCURRENCE_ISSUE_MAPPING.get(issue),
                relatedData);
      }
    });
    return builder.build();
  }

}
