package org.gbif.validation.processor;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Unit test for {@link DataFileSplitter}.
 */
public class DataFileSplitterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final int SPLIT_SIZE = 2;
  private static final int EXPECTED_NUMBER_OF_SPLIT = 3;
  private static final int EXPECTED_NUMBER_OF_LINES = 5;
  private File testFile = FileUtils.getClasspathFile("splitter/original_file.csv");

  @Test
  public void testFileSplit() throws IOException, UnsupportedDataFileException {
    DataFile dataFile = new DataFile(testFile.toPath(), "original_file.csv", FileFormat.TABULAR, "", "");
    DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());

    try {
      List<TabularDataFile> dataFileSplits = DataFileSplitter.splitDataFile(dwcDataFile.getCore(),
              SPLIT_SIZE, folder.newFolder().toPath());
      assertEquals(EXPECTED_NUMBER_OF_SPLIT, dataFileSplits.size());

      //check the offset and numberOfLines
      assertEquals(0, dataFileSplits.get(0).getFileLineOffset().orElse(-1).intValue());
      assertEquals(SPLIT_SIZE, dataFileSplits.get(0).getNumOfLines().intValue());

      assertEquals(2, dataFileSplits.get(1).getFileLineOffset().get().intValue());
      assertEquals(SPLIT_SIZE, dataFileSplits.get(1).getNumOfLines().intValue());

      assertEquals(4, dataFileSplits.get(2).getFileLineOffset().get().intValue());
      //EXPECTED_NUMBER_OF_SPLIT -1 since the last split contains the remaining
      assertEquals(EXPECTED_NUMBER_OF_LINES - ((EXPECTED_NUMBER_OF_SPLIT -1) * SPLIT_SIZE), dataFileSplits.get(2)
              .getNumOfLines().intValue());

    } catch (IOException e) {
      fail(e.getMessage());
    }

  }
}
