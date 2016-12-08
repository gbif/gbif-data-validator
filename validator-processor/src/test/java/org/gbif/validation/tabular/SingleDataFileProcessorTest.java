package org.gbif.validation.tabular;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.ResourceEvaluationManager;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResult;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * TODO move to validator-ws and write a real integration that will send the test file using the ws
 */
public class SingleDataFileProcessorTest {

  // FIXME means test can only run outside
  private static final String DEV_API = "http://api.gbif-dev.org/v1";
  private static final String TEST_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final int NUMBER_OF_ISSUES_EXPECTED = 12;

  @Test
  public void testSingleFileValidation() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_FILE_LOCATION);

    ResourceEvaluationManager manager = new ResourceEvaluationManager(DEV_API, 1000);
    DataFile datafile = new DataFile();
    datafile.setHasHeaders(Optional.of(true));
    datafile.setDelimiterChar('\t');
    datafile.setSourceFileName("myfile.tsv");
    datafile.setRowType(DwcTerm.Occurrence);
    datafile.setFileFormat(FileFormat.TABULAR);
    datafile.setFilePath(testFile.toPath());

    ValidationResult result = manager.evaluate(datafile);

    assertNotNull(result.getResults());
    // we should have 1 ValidationResourceResult which should matches NUMBER_OF_ISSUES_EXPECTED
    assertEquals(NUMBER_OF_ISSUES_EXPECTED, result.getResults().get(0).getIssues().size());
  }

}
