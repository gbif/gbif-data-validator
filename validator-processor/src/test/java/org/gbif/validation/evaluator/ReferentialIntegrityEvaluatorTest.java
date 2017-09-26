package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests related to {@link ReferentialIntegrityEvaluator}
 */
public class ReferentialIntegrityEvaluatorTest {

  private static final String DWC_ARCHIVE = "dwc-data-integrity/dwca";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void dwcaResourceStructureEvaluatorTest() throws IOException, UnsupportedDataFileException {
    ReferentialIntegrityEvaluator riEvaluator = new ReferentialIntegrityEvaluator(DwcTerm.Identification);

    DataFile df = org.gbif.validation.TestUtils.getDwcaDataFile(DWC_ARCHIVE, "dwc-data-integrity-dwca");
    DwcDataFile dwcDf = DataFileFactory.prepareDataFile(df, folder.newFolder().toPath());

    try {
      List<RecordEvaluationResult> results = new ArrayList<>();
      riEvaluator.evaluate(dwcDf, results::add);
      RecordEvaluationResult recordEvaluationResult =  results.get(0);
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("ZZ", recordEvaluationResult.getRecordId());
    } catch (IOException e) {
      fail(e.getMessage());
    }

  }
}
