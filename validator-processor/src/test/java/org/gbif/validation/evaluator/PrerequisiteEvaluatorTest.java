package org.gbif.validation.evaluator;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.gbif.validation.TestUtils.getFirstValidationIssue;
import static org.gbif.validation.source.DataFileFactory.prepareDataFile;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests related to {@link PrerequisiteEvaluator}.
 */
public class PrerequisiteEvaluatorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final String TEST_NO_ID_CSV_FILE_LOCATION = "tabular/no_id.csv";
  private static final PrerequisiteEvaluator prerequisiteEvaluator = new PrerequisiteEvaluator();

  /**
   * Should throw UnsupportedDataFileException: Unable to detect field delimiter
   * @throws IOException
   * @throws UnsupportedDataFileException
   */
  @Test
  public void testCSVWithoutRecordId() throws IOException, UnsupportedDataFileException {
    File testFile = FileUtils.getClasspathFile(TEST_NO_ID_CSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "no-id", FileFormat.TABULAR, "", "");
    DwcDataFile dwcFile = prepareDataFile(dataFile, folder.newFolder().toPath());
    Optional<List<ValidationResultElement>> result = prerequisiteEvaluator.evaluate(dwcFile);

    assertTrue(result.isPresent());
    assertEquals(EvaluationType.CORE_ROWTYPE_UNDETERMINED,  getFirstValidationIssue(result.get()).getIssue());
  }
}
