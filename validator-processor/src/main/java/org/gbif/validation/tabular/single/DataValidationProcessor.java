package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResultBuilders.RecordsValidationResultElementBuilder;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;

import java.util.List;
import java.util.Optional;

public class DataValidationProcessor  {

  private final RecordEvaluator recordEvaluator;

  private final SimpleValidationCollector collector;
  private final TermsFrequencyCollector metricsCollector;
  private final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

  private long line;

  public DataValidationProcessor(List<Term> terms, RecordEvaluator recordEvaluator,
                                 Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
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
      interpretedTermsCountCollector.ifPresent(c -> c.collect(recEvalResult));
      line++;
      return recEvalResult;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public RecordsValidationResultElement getValidationResult(String fileName, Term rowType) {
    return RecordsValidationResultElementBuilder
            .of(fileName, rowType, line)
            .withIssues(collector.getAggregatedCounts(), collector.getSamples())
            .withTermsFrequency(metricsCollector.getTermFrequency())
            .withInterpretedValueCounts(interpretedTermsCountCollector.isPresent() ? interpretedTermsCountCollector.get().getInterpretedCounts() : null)
            .build();
  }

}
