package org.gbif.validation.evaluator.structure;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResultElement;

import java.io.File;
import java.util.Optional;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

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

    Optional<ValidationResultElement> result = riEvaluator.evaluate(df);
    assertEquals(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION, result.get().getIssues().get(0).getIssue());
  }
}
