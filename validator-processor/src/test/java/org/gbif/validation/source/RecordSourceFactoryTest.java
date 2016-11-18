package org.gbif.validation.source;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit tests for RecordSourceFactory
 */
public class RecordSourceFactoryTest {

  private static final String TEST_TSV_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final String TEST_DWC_FILE_LOCATION = "dwc-archive";

  @Test
  public void testPrepareSourceDwc() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setFilePath(testFile.toPath());

    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDataFiles.size());

    //all components should points to the parent DateFile
    preparedDataFiles.forEach( df -> assertEquals(dataFile, df.isAlternateViewOf().get()));

  }

  @Test
  public void testPrepareSourceTabular() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_TSV_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.TABULAR);
    dataFile.setFilePath(testFile.toPath());
    dataFile.setHasHeaders(Optional.of(true));

    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);

    assertEquals(1, preparedDataFiles.size());
    DataFile preparedDataFile = preparedDataFiles.get(0);
    assertEquals('\t', preparedDataFile.getDelimiterChar().charValue());
    assertEquals(DwcTerm.Occurrence, preparedDataFile.getRowType());
  }

}
