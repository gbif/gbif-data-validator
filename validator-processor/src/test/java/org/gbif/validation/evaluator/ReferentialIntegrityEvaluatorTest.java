package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class ReferentialIntegrityEvaluatorTest {

  private static final File DWC_ARCHIVE = FileUtils.getClasspathFile("dwc-data-integrity/dwca");

  @Test
  public void dwcaResourceStructureEvaluatorTest() {
    ReferentialIntegrityEvaluator riEvaluator = new ReferentialIntegrityEvaluator(DwcTerm.Identification);

    DataFile df = new DataFile();
    df.setFileFormat(FileFormat.DWCA);
    df.setFilePath(DWC_ARCHIVE.toPath());
    df.setNumOfLines(10);
    df.setSourceFileName("dwc-data-integrity-dwca");

    try {
      Optional<Stream<RecordEvaluationResult>> result = riEvaluator.evaluate(df);
      RecordEvaluationResult recordEvaluationResult =  result.get().findFirst().get();
      RecordEvaluationResultDetails recordEvaluationResultDetails = recordEvaluationResult.getDetails().get(0);
      assertEquals(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION, recordEvaluationResultDetails.getEvaluationType());
      assertEquals("ZZ", recordEvaluationResult.getRecordId());
    } catch (IOException e) {
      fail(e.getMessage());
    }


  }
}
