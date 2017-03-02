package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TabularFileReaderTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final String TEST_TSV_FILE_LOCATION = "validator_test_file_all_issues.tsv";

  @Test
  public void testCsvReading() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_TSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "validator_test_file_all_issues.tsv", FileFormat.TABULAR, "");

    TabularDataFile tsvTabularDataFile =
            DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath()).getCore();

    try(RecordSource recordSource = RecordSourceFactory.fromTabularDataFile(tsvTabularDataFile)) {
      assertEquals("http://coldb.mnhn.fr/catalognumber/mnhn/p/p00501568", recordSource.read()[0]);
    }
  }
}
