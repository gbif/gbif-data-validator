package org.gbif.validation.source;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for RecordSourceFactory
 */
public class RecordSourceFactoryTest {

  private static final String TEST_TSV_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final String TEST_DWC_FILE_LOCATION = "dwca/dwca-taxon";

  @Test
  public void testPrepareSourceDwc() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setFilePath(testFile.toPath());

    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);
    //the test Dwc folder contains 1 core + 2 extensions
    assertEquals(3, preparedDataFiles.size());

    //all components should points to the parent DataFile
    preparedDataFiles.forEach( df -> assertEquals(dataFile, df.getParent().get()));

    // the filePath should point to the component file
    assertTrue(StringUtils.endsWith(preparedDataFiles.get(0).getFilePath().toString(), ".txt"));
  }

  @Test
  public void testfromDataFile() throws IOException {
    DataFile dataFile = new DataFile();

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setFilePath(testFile.toPath());
    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);

    DataFile taxonDataFile = preparedDataFiles.stream().filter(df -> df.getRowType() == DwcTerm.Taxon).findFirst().get();
    try(RecordSource rs = RecordSourceFactory.fromDataFile(taxonDataFile).get()){
      assertEquals("1559060", rs.read()[0]);
    }
  }

  @Test
  public void testPrepareSourceTabular() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_TSV_FILE_LOCATION);
    DataFile dataFile = new DataFile();
    dataFile.setFileFormat(FileFormat.TABULAR);
    dataFile.setFilePath(testFile.toPath());
    dataFile.setHasHeaders(true);

    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);

    assertEquals(1, preparedDataFiles.size());
    DataFile preparedDataFile = preparedDataFiles.get(0);
    assertEquals('\t', preparedDataFile.getDelimiterChar().charValue());
    assertEquals(DwcTerm.Occurrence, preparedDataFile.getRowType());
  }

}
