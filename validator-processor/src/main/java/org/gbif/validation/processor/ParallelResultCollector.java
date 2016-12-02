package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordMetricsCollector;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.collector.RecordEvaluationResultCollector;
import org.gbif.validation.collector.TermsFrequencyCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ParallelResultCollector {

  final RecordMetricsCollector metricsCollector;
  final RecordEvaluationResultCollector resultsCollector;
  final Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector;

  final List<ResultsCollector> recordsCollectors;

  public ParallelResultCollector(List<Term> termsColumnsMapping, Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
    metricsCollector = new TermsFrequencyCollector(termsColumnsMapping, true);
    resultsCollector = new RecordEvaluationResultCollector(RecordEvaluationResultCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE, true);
    recordsCollectors = new ArrayList<>();
    recordsCollectors.add(resultsCollector);
    this.interpretedTermsCountCollector = interpretedTermsCountCollector;
    interpretedTermsCountCollector.ifPresent(c -> recordsCollectors.add(c));
  }

  public RecordsValidationResultElement toResult(DataFile dataFile, String resultingFileName){
    return ValidationResultBuilders.RecordsValidationResultElementBuilder
      .of(resultingFileName, dataFile.getRowType(),
          dataFile.getNumOfLines() - (dataFile.isHasHeaders().orElse(false) ? 1l : 0l))
      .withIssues(resultsCollector.getAggregatedCounts(), resultsCollector.getSamples())
      .withTermsFrequency(metricsCollector.getTermFrequency())
      .withInterpretedValueCounts(interpretedTermsCountCollector.isPresent() ? interpretedTermsCountCollector.get().getInterpretedCounts() : null)
      .build();
  }
}
