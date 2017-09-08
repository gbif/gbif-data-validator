package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Unit tests related to @{UniquenessEvaluator}
 */
public class UniquenessEvaluatorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final File DWC_ARCHIVE = FileUtils.getClasspathFile("dwc-data-integrity/dwca");

  @Test
  public void testUniqueness() throws UnsupportedDataFileException {

    DataFile df = DataFileFactory.newDataFile(DWC_ARCHIVE.toPath(), "dwc-data-integrity-dwca",
            FileFormat.DWCA, "", "");

    try {
      Path testFolder = folder.newFolder().toPath();
      DwcDataFile dwcaContent = DataFileFactory.prepareDataFile(df,testFolder);

      UniquenessEvaluator ue = new UniquenessEvaluator(DwcTerm.Occurrence, false, testFolder);
      List<RecordEvaluationResult> results = new ArrayList<>();
      ue.evaluate(dwcaContent, results::add);

      RecordEvaluationResult recordEvaluationResult = results.get(0);
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("19", recordEvaluationResult.getRecordId());

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
