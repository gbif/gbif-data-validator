package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.source.DataFileFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Unit tests related to @{UniquenessEvaluator}
 */
public class UniquenessEvaluatorTest {

  private static final File DWC_ARCHIVE = FileUtils.getClasspathFile("dwc-data-integrity/dwca");

  @Test
  public void testUniqueness() {

    DataFile df = DataFileFactory.newDataFile(DWC_ARCHIVE.toPath(), "dwc-data-integrity-dwca",
            FileFormat.DWCA, "");

    try {
      List<TabularDataFile> dwcaContent = DataFileFactory.prepareDataFile(df);
      Optional<TabularDataFile> occDf = dwcaContent.stream()
              .filter(_df -> _df.getRowType() == DwcTerm.Occurrence)
              .findFirst();

      UniquenessEvaluator ue = new UniquenessEvaluator(1, false);
      Optional<Stream<RecordEvaluationResult>> uniquenessEvaluatorResult = ue.evaluate(occDf.get());

      RecordEvaluationResult recordEvaluationResult = uniquenessEvaluatorResult.get().findFirst().get();
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("19", recordEvaluationResult.getRecordId());

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
