package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;
import org.gbif.validation.tabular.RecordSourceFactory;

import java.io.File;

public class DataValidationProcessor  {

  private final RecordEvaluator recordEvaluator;

  private final SimpleValidationCollector collector;
  private final TermsFrequencyCollector metricsCollector;
  private final InterpretedTermsCountCollector interpretedTermsCountCollector;

  private int line;

  //TODO Should interpretedTermsCountCollector be nullable?
  public DataValidationProcessor(Term[] terms, RecordEvaluator recordEvaluator,
                                 InterpretedTermsCountCollector interpretedTermsCountCollector) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(SimpleValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    metricsCollector = new TermsFrequencyCollector(terms, false);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
  }


  public RecordEvaluationResult process(String[] record) {
    try {
      metricsCollector.collect(record);
      //TODO: long vs int
      RecordEvaluationResult recEvalResult = recordEvaluator.evaluate(Integer.toUnsignedLong(line), record);
      collector.collect(recEvalResult);
      interpretedTermsCountCollector.collect(recEvalResult);
      line++;
      return recEvalResult;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public ValidationResult getValidationResult() {
    //FIXME the Status and indexeable should be decided by a another class somewhere
    return ValidationResult.Builder
      .of(true, FileFormat.TABULAR, line, ValidationProfile.GBIF_INDEXING_PROFILE)
      .withIssues(collector.getAggregatedCounts(), collector.getSamples())
      .withTermsFrequency(metricsCollector.getTermFrequency())
      .withInterpretedValueCounts(interpretedTermsCountCollector.getInterpretedCounts())
      .build();
  }


}
