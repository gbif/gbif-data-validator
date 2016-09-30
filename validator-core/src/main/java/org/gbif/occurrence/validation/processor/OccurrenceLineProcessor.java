package org.gbif.occurrence.validation.processor;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class to process one text line that represents a occurrence record.
 */
public class OccurrenceLineProcessor implements RecordProcessor {

  private final OccurrenceInterpreter interpreter;

  /**
   * Default constructor, builds an instance using a OccurrenceInterpreter class.
   * @param interpreter occurrence interpreter
   */
  public OccurrenceLineProcessor(OccurrenceInterpreter interpreter) {
    this.interpreter = interpreter;
  }

  @Override
  public RecordInterpretionBasedEvaluationResult process(@Nullable String id, Map<Term, String> record) {
    //TODO maybe we should copy the fields?
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult(id, interpreter.interpret(verbatimOccurrence));
  }

  /**
   * Creates a RecordInterpretionBasedEvaluationResult from an OccurrenceInterpretationResult.
   * Responsible to to put the related data (e.g. field + current value) into the RecordInterpretionBasedEvaluationResult
   * instance.
   */
  private static RecordInterpretionBasedEvaluationResult toEvaluationResult(String id, OccurrenceInterpretationResult result) {

    RecordInterpretionBasedEvaluationResult.Builder builder = new RecordInterpretionBasedEvaluationResult.Builder();
    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();

    builder.withIdentifier(id);
    result.getUpdated().getIssues().forEach( issue -> {
      if (InterpretationRemarksDefinition.REMARKS_MAP.containsKey(issue)) {
        Map<Term, String> relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue)
          .stream()
          .filter(t -> verbatimFields.get(t) != null)
          .collect(Collectors.toMap(Function.identity(), t -> verbatimFields.get(t)));
        builder.addDetail(issue, relatedData);
      }
    });
    return builder.build();
  }
}
