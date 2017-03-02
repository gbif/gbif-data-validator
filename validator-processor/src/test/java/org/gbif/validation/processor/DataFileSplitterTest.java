package org.gbif.validation.processor;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;
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

  private File testFile = FileUtils.getClasspathFile("splitter/original_file.csv");

  @Test
  public void testFileSplit() throws IOException, UnsupportedDataFileException {
    DataFile dataFile = new DataFile(testFile.toPath(), "original_file.csv", FileFormat.TABULAR, "");
    DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());

    try {
      List<TabularDataFile> dataFileSplits = DataFileSplitter.splitDataFile(dwcDataFile.getCore(),
              2, folder.newFolder().toPath());
      assertEquals(3, dataFileSplits.size());

      //check the offset
      assertEquals(0, dataFileSplits.get(0).getFileLineOffset().get().intValue());
      assertEquals(2, dataFileSplits.get(1).getFileLineOffset().get().intValue());
      assertEquals(4, dataFileSplits.get(2).getFileLineOffset().get().intValue());

    } catch (IOException e) {
      fail(e.getMessage());
    }

  }
}
