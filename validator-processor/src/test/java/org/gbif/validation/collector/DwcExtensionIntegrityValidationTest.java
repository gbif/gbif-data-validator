package org.gbif.validation.collector;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.source.DataFileFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import static org.gbif.validation.collector.DwcExtensionIntegrityValidation.collectUnlinkedExtensions;

/**
 * Tests class to validate data integrity in Dwc files.
 */
public class DwcExtensionIntegrityValidationTest {

  private static final String RESOURCES_DIR = "dwc-data-integrity";

  /**
   * Gets a test core file from the specified testDir.
   */
  private static TabularDataFile getCoreTestFileDescriptor(String testDir) throws IOException {
    return getTestDataFile(testDir, "core.txt");
  }

  /**
   * Gets a test extension file from the specified testDir.
   */
  private static TabularDataFile getExtensionTestFileDescriptor(String testDir) throws IOException {
    return getTestDataFile(testDir, "ext.txt");
  }

  /**
   * Utility class to create data file descriptors from tests files.
   */
  private static TabularDataFile getTestDataFile(String testDir, String testFileName) throws IOException {
    try {
      URL testFileUrl = Resources.getResource(Paths.get(RESOURCES_DIR, testDir, testFileName).toString());
      DataFile dataFile = new DataFile(Paths.get(testFileUrl.toURI()), testFileName, FileFormat.TABULAR, "");
      return DataFileFactory.prepareDataFile(dataFile).stream().findFirst().get();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Utility method to build simple data integrity tests.
   */
  private static void buildIntegrityTest(String testDir, int coreColumn, int extColumn, int numSamples)
    throws IOException {
    List<String> unLinked  = collectUnlinkedExtensions(getCoreTestFileDescriptor(testDir), coreColumn, //core, column
                                                       getExtensionTestFileDescriptor(testDir), extColumn, //ext, column
                                                       numSamples); //# of samples
    Assert.assertEquals(unLinked.size(), numSamples);
  }

  /**
   * Utility method to build simple data integrity tests.
   */
  private static void buildIntegrityInSameFileTest(String testDir, int coreColumn, int parentColumn, int numSamples)
    throws IOException {
    List<String> unLinked  = collectUnlinkedExtensions(getCoreTestFileDescriptor(testDir), coreColumn, //core, column
                                                       getCoreTestFileDescriptor(testDir), parentColumn, //ext, column
                                                       numSamples); //# of samples
    Assert.assertEquals(unLinked.size(), numSamples);
  }

  /**
   * Tests that 2 extension columns are missing in the core file.
   */
  @Test
  public void collectUnlinkedExtensionsTest() throws IOException {
    buildIntegrityTest("missing2CoreIDs", 0, 0, 2);
  }

  /**
   * Tests that correct number of samples are collected.
   */
  @Test
  public void collectUnlinkedExtensionsSamplingTest() throws IOException {
    buildIntegrityTest("missing2CoreIDs", 0, 0, 1);
  }

  /**
   * Tests that 2 extension columns are missing in columns different to the first columns in core and extension files.
   */
  @Test
  public void collectUnlinkedExtensionsNoInitialColumnsTest() throws IOException {
    buildIntegrityTest("integrityOn3rdColumn", 2, 1, 2);
  }

  /**
   * Tests that 2 extension columns are missing in columns different to the first columns in core and extension files.
   */
  @Test
  public void collectUnlinkedParentIdTest() throws IOException {
    buildIntegrityInSameFileTest("integrityInSameFile", 0, 4, 1);
  }

}
