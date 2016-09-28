package org.gbif.occurrence.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.model.StructureEvaluationDetailType;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;
import org.gbif.occurrence.validation.util.TempTermsUtils;

import java.io.File;
import java.text.MessageFormat;
import java.util.Map;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordProcessor recordProcessor;
  private final SimpleValidationCollector collector;

  public SingleDataFileProcessor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
    collector = new SimpleValidationCollector();
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
        collector.accumulate(recordProcessor.process(Long.toString(line), record));

        if (record.size() != expectedNumberOfColumn) {
          collector.accumulate(toColumnCountMismatchEvaluationResult(line, expectedNumberOfColumn, record.size()));
        }
      }

      return new DataFileValidationResult(collector.getAggregatedCounts(), collector.getSamples());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Creates a RecordStructureEvaluationResult instance for a column count mismatch.
   *
   * @param lineNumber
   * @param expectedColumnCount
   * @param actualColumnCount
   * @return
   */
  private static RecordStructureEvaluationResult toColumnCountMismatchEvaluationResult(long lineNumber, int expectedColumnCount,
                                                                                       int actualColumnCount) {
    //FIXME record line number
    return new RecordStructureEvaluationResult.Builder().addDetail(StructureEvaluationDetailType.RECORD_STRUCTURE,
            MessageFormat.format("Column count mismatch: expected {0} columns, got {1} columns",
                    expectedColumnCount, actualColumnCount)).build();
  }
}
