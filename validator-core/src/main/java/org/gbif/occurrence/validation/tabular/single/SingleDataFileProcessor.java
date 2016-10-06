package org.gbif.occurrence.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.model.StructureEvaluationDetailType;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;
import org.gbif.occurrence.validation.util.TempTermsUtils;

import java.io.File;
import java.text.MessageFormat;
import java.util.Map;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordEvaluator recordEvaluator;
  private final SimpleValidationCollector collector;

  public SingleDataFileProcessor(RecordEvaluator recordEvaluator) {
    this.recordEvaluator = recordEvaluator;
    collector = new SimpleValidationCollector(ResultsCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE);
  }

  @Override
  public DataFileValidationResult process(DataFile dataFile) {

    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(),
            dataFile.isHasHeaders(), TempTermsUtils.buildTermMapping(dataFile.getColumns()))) {
      int expectedNumberOfColumn = dataFile.getColumns().length;
      Map<Term, String> record;
      long line = dataFile.isHasHeaders() ? 1 : 0;
      while ((record = recordSource.read()) != null) {
        line++;
        collector.accumulate(recordEvaluator.process(Long.toString(line), record));


      }

      return new DataFileValidationResult(collector.getAggregatedCounts(), collector.getSamples());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
