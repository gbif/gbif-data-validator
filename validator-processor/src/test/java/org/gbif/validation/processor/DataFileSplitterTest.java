package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link DataFileSplitter}.
 */
public class DataFileSplitterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private File testFile = FileUtils.getClasspathFile("splitter/original_file.csv");

  @Test
  public void testFileSplit(){
    //testFile
    DataFile dataFile = new DataFile();
    dataFile.setFilePath(testFile.toPath());
    dataFile.setRowType(DwcTerm.Occurrence);
    dataFile.setNumOfLines(4);
    dataFile.setHasHeaders(false);

    try {
      List<DataFile> dataFileSplits = DataFileSplitter.splitDataFile(dataFile, 2, folder.newFolder().toPath());
      assertEquals(2, dataFileSplits.size());

      //check the offset
      assertEquals(0, dataFileSplits.get(0).getFileLineOffset().get().intValue());
      assertEquals(2, dataFileSplits.get(1).getFileLineOffset().get().intValue());

    } catch (IOException e) {
      fail(e.getMessage());
    }

  }
}
