package org.gbif.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.source.RecordSourceFactory;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final DataValidationProcessor dataValidationProcessor;

  //TODO Should interpretedTermsCountCollector be nullable?
  public SingleDataFileProcessor(List<Term> terms, RecordEvaluator recordEvaluator,
                                 InterpretedTermsCountCollector interpretedTermsCountCollector) {
    dataValidationProcessor = new DataValidationProcessor(terms, recordEvaluator, interpretedTermsCountCollector);
  }

  @Override
  public ValidationResult process(DataFile dataFile) throws IOException {

    try (RecordSource recordSource = RecordSourceFactory.fromDataFile(dataFile).orElse(null)) {
      String[] record;
      while ((record = recordSource.read()) != null) {
        dataValidationProcessor.process(record);
      }

      DataFile scopedDataFile = dataFile.isAlternateViewOf().orElse(dataFile);

      //FIXME the Status and indexeable should be decided by a another class somewhere
      return ValidationResultBuilders.Builder.of(true, scopedDataFile.getSourceFileName(),
              scopedDataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE)
              .withResourceResult(
                      dataValidationProcessor.getValidationResult(
                              StringUtils.isNotBlank(dataFile.getSourceFileName()) ? dataFile.getSourceFileName() :
                                      scopedDataFile.getSourceFileName(),
                              dataFile.getRowType())).build();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
