package org.gbif.validation.tabular.single;

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
      return ValidationResult.of(collector.getAggregatedCounts().isEmpty() ? ValidationResult.Status.OK : ValidationResult.Status.FAILED,
              true, FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE,
              dataFile.getNumOfLines(),
              collector.getAggregatedCounts(), collector.getSamples());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
