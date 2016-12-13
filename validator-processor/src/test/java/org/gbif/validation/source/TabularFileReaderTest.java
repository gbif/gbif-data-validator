package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TabularFileReaderTest {

  private static final String TEST_TSV_FILE_LOCATION = "validator_test_file_all_issues.tsv";

  @Test
  public void testCsvReading() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_TSV_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.TABULAR);
    dataFile.setFilePath(testFile.toPath());
    dataFile.setHasHeaders(true);
    dataFile.setDelimiterChar('\t');

    //all components should points to the parent DataFile
    Optional<RecordSource> rc = RecordSourceFactory.fromDataFile(dataFile);
    try(RecordSource recordSource = rc.get()) {
      assertEquals("http://coldb.mnhn.fr/catalognumber/mnhn/p/p00501568", recordSource.read()[0]);
    }
  }
}
