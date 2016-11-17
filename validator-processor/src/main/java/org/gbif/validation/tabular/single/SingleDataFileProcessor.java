package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.source.RecordSourceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final DataValidationProcessor dataValidationProcessor;

  public SingleDataFileProcessor(List<Term> terms, RecordEvaluator recordEvaluator,
                                 Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector) {
    dataValidationProcessor = new DataValidationProcessor(terms, recordEvaluator, interpretedTermsCountCollector);
  }

  @Override
  public RecordsValidationResultElement process(DataFile dataFile) throws IOException {

    try (RecordSource recordSource = RecordSourceFactory.fromDataFile(dataFile).orElse(null)) {
      String[] record;
      while ((record = recordSource.read()) != null) {
        dataValidationProcessor.process(record);
      }

      DataFile scopedDataFile = dataFile.isAlternateViewOf().orElse(dataFile);
      return dataValidationProcessor.getValidationResult(
              StringUtils.isNotBlank(dataFile.getSourceFileName()) ? dataFile.getSourceFileName() :
                      scopedDataFile.getSourceFileName(),
              dataFile.getRowType());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
