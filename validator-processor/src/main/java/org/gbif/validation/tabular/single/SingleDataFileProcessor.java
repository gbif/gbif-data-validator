package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.ResultsCollector;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.tabular.RecordSourceFactory;

import java.io.File;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordEvaluator recordEvaluator;
  private final SimpleValidationCollector collector;
  private final SimpleTermsFrequencyCollector metricsCollector;

  public SingleDataFileProcessor(Term[] terms, RecordEvaluator recordEvaluator) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(ResultsCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
    metricsCollector = new SimpleTermsFrequencyCollector(terms);
  }

  @Override
  public ValidationResult process(DataFile dataFile) {

    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(),
            dataFile.isHasHeaders())) {
      String[] record;
      long line = dataFile.isHasHeaders() ? 1 : 0;
      while ((record = recordSource.read()) != null) {
        line++;
        metricsCollector.collect(record);
        collector.accumulate(recordEvaluator.evaluate(line, record));
      }

      //FIXME the Status and indexeable should be decided by a another class somewhere
      return ValidationResult.Builder
              .of(true, FileFormat.TABULAR, dataFile.getNumOfLines(), ValidationProfile.GBIF_INDEXING_PROFILE)
              .withIssues(collector.getAggregatedCounts(), collector.getSamples())
              .withTermsFrequency(metricsCollector.getTermFrequency())
              .build();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
