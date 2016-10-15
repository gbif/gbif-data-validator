package org.gbif.occurrence.validation.tabular.single;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.api.model.ValidationProfile;
import org.gbif.occurrence.validation.api.model.ValidationResult;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;

import java.io.File;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordEvaluator recordEvaluator;
  private final SimpleValidationCollector collector;

  public SingleDataFileProcessor(RecordEvaluator recordEvaluator) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(ResultsCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
  }

  @Override
  public ValidationResult process(DataFile dataFile) {

    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(),
            dataFile.isHasHeaders())) {
      String[] record;
      long line = dataFile.isHasHeaders() ? 1 : 0;
      while ((record = recordSource.read()) != null) {
        line++;
        collector.accumulate(recordEvaluator.evaluate(line, record));
      }

      //FIXME the Status and indexeable should be decided by a another class somewhere
      return new ValidationResult(
              collector.getAggregatedCounts().isEmpty() ? ValidationResult.Status.OK : ValidationResult.Status.FAILED,
              true, ValidationProfile.GBIF_INDEXING_PROFILE, collector.getAggregatedCounts(), collector.getSamples());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
