package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.api.model.ValidationResult.RecordsValidationResourceResultBuilder;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;

import java.util.List;

public class DataValidationProcessor  {

  private final RecordEvaluator recordEvaluator;

  private final SimpleValidationCollector collector;
  private final TermsFrequencyCollector metricsCollector;
  private final InterpretedTermsCountCollector interpretedTermsCountCollector;

  private long line;

  //TODO Should interpretedTermsCountCollector be nullable?
  public DataValidationProcessor(List<Term> terms, RecordEvaluator recordEvaluator,
                                 InterpretedTermsCountCollector interpretedTermsCountCollector) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(SimpleValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    metricsCollector = new TermsFrequencyCollector(terms, false);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
  }

  public RecordEvaluationResult process(String[] record) {
    try {
      metricsCollector.collect(record);
      RecordEvaluationResult recEvalResult = recordEvaluator.evaluate(line, record);
      collector.collect(recEvalResult);
      interpretedTermsCountCollector.collect(recEvalResult);
      line++;
      return recEvalResult;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public ValidationResult.RecordsValidationResourceResult getValidationResult() {
    return RecordsValidationResourceResultBuilder
            .of("", line)
            .withIssues(collector.getAggregatedCounts(), collector.getSamples())
            .withTermsFrequency(metricsCollector.getTermFrequency())
            .withInterpretedValueCounts(interpretedTermsCountCollector.getInterpretedCounts())
            .build();
  }

}
