package org.gbif.validation.processor;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link DataFileSplitter}.
 */
public class DataFileSplitterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private File testFile = FileUtils.getClasspathFile("splitter/original_file.csv");

  @Ignore("missing classes")
  public void testFileSplit() throws IOException {
    //testFile
//    DataFile dataFile = new DataFile(testFile.toPath(), "original_file.csv", FileFormat.TABULAR, "");
//    Optional<TabularDataFile> tabDataFile = DataFileFactory.prepareDataFile(dataFile).stream().findFirst();
//
//    try {
//      List<TabularDataFile> dataFileSplits = DataFileSplitter.splitDataFile(tabDataFile.get(), 2, folder.newFolder().toPath());
//      assertEquals(2, dataFileSplits.size());
//
//      //check the offset
//      assertEquals(0, dataFileSplits.get(0).getFileLineOffset().get().intValue());
//      assertEquals(2, dataFileSplits.get(1).getFileLineOffset().get().intValue());
//      assertEquals(2, dataFileSplits.get(2).getFileLineOffset().get().intValue());
//
//    } catch (IOException e) {
//      fail(e.getMessage());
//    }

  }
}
