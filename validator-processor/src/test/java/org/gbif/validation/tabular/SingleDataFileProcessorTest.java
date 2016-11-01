package org.gbif.validation.tabular;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationResult;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * TODO move to validator-ws and write a real integration that will send the test file using the ws
 */
public class SingleDataFileProcessorTest {

  private static final String DEV_API = "http://api.gbif-dev.org/v1";
  private static final String TEST_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final int NUMBER_OF_ISSUES_EXPECTED = 12;

  @Test
  public void testSingleFileValidation() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_FILE_LOCATION);

    DataFileProcessorFactory factory = new DataFileProcessorFactory(DEV_API);
    DataFile datafile = new DataFile();
    datafile.setHasHeaders(true);
    datafile.setDelimiterChar('\t');
    datafile.setSourceFileName("myfile.tsv");
    datafile.setFileFormat(FileFormat.TABULAR);
    datafile.setFileName(testFile.getAbsolutePath());

    DataFileProcessor processor = factory.create(datafile);
    ValidationResult result = processor.process(datafile);

    assertNotNull(result.getResults());
    // we should have 1 ValidationResourceResult which should matches NUMBER_OF_ISSUES_EXPECTED
    assertEquals(NUMBER_OF_ISSUES_EXPECTED, result.getResults().get(0).getIssues().size());
  }

}
