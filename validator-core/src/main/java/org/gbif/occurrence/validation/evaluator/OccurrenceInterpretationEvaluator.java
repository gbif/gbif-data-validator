package org.gbif.occurrence.validation.evaluator;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.model.RecordInterpretationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.model.StructureEvaluationDetailType;

import java.text.MessageFormat;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class to process one text line that represents a occurrence record.
 */
public class OccurrenceInterpretationEvaluator implements RecordEvaluator<RecordInterpretationResult> {

  private final OccurrenceInterpreter interpreter;
  private final String[] fields;

  /**
   * Default constructor, builds an instance using a OccurrenceInterpreter class.
   * @param interpreter occurrence interpreter
   */
  public OccurrenceInterpretationEvaluator(OccurrenceInterpreter interpreter, String[] fields) {
    this.interpreter = interpreter;
    this.fields = fields;
  }

  @Override
  public RecordInterpretationResult process(@Nullable String id, Map<Term, String> record) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult(id, interpreter.interpret(verbatimOccurrence));
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
  private static RecordInterpretationResult toEvaluationResult(String id, OccurrenceInterpretationResult result) {

    RecordInterpretationResult.Builder builder = new RecordInterpretationResult.Builder();
    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();

    builder.withIdentifier(id);
    result.getUpdated().getIssues().forEach( issue -> {
      if (InterpretationRemarksDefinition.REMARKS_MAP.containsKey(issue)) {
        Map<Term, String> relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue)
          .stream()
          .filter(t -> verbatimFields.get(t) != null)
          .collect(Collectors.toMap(Function.identity(), verbatimFields::get));
        builder.addDetail(issue, relatedData);
      }
    });
    return builder.build();
  }

}