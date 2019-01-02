package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Unit tests related to @{DataUniquenessEvaluator}
 */
public class DataUniquenessEvaluatorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  /**
   * occurrence.txt contains two records with a dwc:occurrenceId of o-19.
   */
  @Test
  public void testUniquenessOccurrence() throws UnsupportedDataFileException {
    File dwcArchive = FileUtils.getClasspathFile("dwc-data-integrity/dwca");

    DataFile df = DataFileFactory.newDataFile(dwcArchive.toPath(), "dwc-data-integrity-dwca",
      FileFormat.DWCA, "", "");

    try {
      Path testFolder = folder.newFolder().toPath();
      DwcDataFile dwcaContent = DataFileFactory.prepareDataFile(df,testFolder);

      DataUniquenessEvaluator ue = new DataUniquenessEvaluator(false, testFolder);
      List<RecordEvaluationResult> results = new ArrayList<>();
      ue.evaluate(dwcaContent, results::add);

      RecordEvaluationResult recordEvaluationResult = results.get(0);
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("i19", recordEvaluationResult.getRecordId());
      assertEquals("o-19", recordEvaluationResult.getDetails().get(0).getRelatedData().get(DwcTerm.occurrenceID));

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  /**
   * occurrence.txt contains two records with a dwc:occurrenceId of SDA:AVE:PEDH:TIB:21.
   */
  @Test
  public void testUniquenessEvent() throws UnsupportedDataFileException {
    File dwcArchive = FileUtils.getClasspathFile("dwc-data-integrity/dwca-event-duplicate-occurrenceId");

    DataFile df = DataFileFactory.newDataFile(dwcArchive.toPath(), "dwc-event-data-integrity-dwca",
      FileFormat.DWCA, "", "");

    try {
      Path testFolder = folder.newFolder().toPath();
      DwcDataFile dwcaContent = DataFileFactory.prepareDataFile(df,testFolder);

      DataUniquenessEvaluator ue = new DataUniquenessEvaluator(false, testFolder);
      List<RecordEvaluationResult> results = new ArrayList<>();
      ue.evaluate(dwcaContent, results::add);

      RecordEvaluationResult recordEvaluationResult = results.get(0);
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("TIB-P2", recordEvaluationResult.getRecordId());
      assertEquals("SDA:AVE:PEDH:TIB:21", recordEvaluationResult.getDetails().get(0).getRelatedData().get(DwcTerm.occurrenceID));

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
