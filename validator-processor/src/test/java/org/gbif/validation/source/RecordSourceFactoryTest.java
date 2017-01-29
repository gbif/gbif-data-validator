package org.gbif.validation.source;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for RecordSourceFactory
 */
public class RecordSourceFactoryTest {

  private static final String TEST_TSV_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final String TEST_DWC_FILE_LOCATION = "dwca/dwca-taxon";

  @Test
  public void testfromDataFile() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_DWC_FILE_LOCATION);

    DataFile dataFile = new DataFile(testFile.toPath(), "dwca-taxon", FileFormat.DWCA, "");

    List<TabularDataFile> preparedDataFiles = DataFileFactory.prepareDataFile(dataFile);

    TabularDataFile taxonDataFile = preparedDataFiles.stream().filter(df -> df.getRowType() == DwcTerm.Taxon).findFirst().get();
    try (RecordSource rs = RecordSourceFactory.fromTabularDataFile(taxonDataFile).get()) {
      assertEquals("1559060", rs.read()[0]);
    }
  }

  @Test
  public void testPrepareSourceTabular() throws IOException {

    File testFile = FileUtils.getClasspathFile(TEST_TSV_FILE_LOCATION);
    DataFile dataFile = new DataFile(testFile.toPath(), "validator_test_file_all_issues.tsv", FileFormat.TABULAR, "");

    List<TabularDataFile> preparedDataFiles = DataFileFactory.prepareDataFile(dataFile);

    assertEquals(1, preparedDataFiles.size());
    TabularDataFile preparedDataFile = preparedDataFiles.get(0);
    assertEquals('\t', preparedDataFile.getDelimiterChar().charValue());
    assertEquals(DwcTerm.Occurrence, preparedDataFile.getRowType());
  }

}
