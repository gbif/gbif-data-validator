package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.DwcTerm;
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
import java.util.Arrays;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordEvaluator recordEvaluator;

  private final SimpleValidationCollector collector;
  private final TermsFrequencyCollector metricsCollector;
  private final InterpretedTermsCountCollector interpretedTermsCountCollector;

  public SingleDataFileProcessor(Term[] terms, RecordEvaluator recordEvaluator) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(SimpleValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    metricsCollector = new TermsFrequencyCollector(terms, false);
    interpretedTermsCountCollector = new InterpretedTermsCountCollector(Arrays.asList(DwcTerm.year), false);
  }

  @Override
  public ValidationResult process(DataFile dataFile) {

    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(),
            dataFile.isHasHeaders())) {
      String[] record;
      long line = dataFile.isHasHeaders() ? 1 : 0;
      RecordEvaluationResult recEvalResult;
      while ((record = recordSource.read()) != null) {
        line++;
        metricsCollector.collect(record);
        recEvalResult = recordEvaluator.evaluate(line, record);
        collector.collect(recEvalResult);
        interpretedTermsCountCollector.collect(recEvalResult);
      }

      //FIXME the Status and indexeable should be decided by a another class somewhere
      return ValidationResult.Builder
              .of(true, FileFormat.TABULAR, dataFile.getNumOfLines() - (dataFile.isHasHeaders() ? 1 : 0), ValidationProfile.GBIF_INDEXING_PROFILE)
              .withIssues(collector.getAggregatedCounts(), collector.getSamples())
              .withTermsFrequency(metricsCollector.getTermFrequency())
              .withInterpretedValueCounts(interpretedTermsCountCollector.getInterpretedCounts())
              .build();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
