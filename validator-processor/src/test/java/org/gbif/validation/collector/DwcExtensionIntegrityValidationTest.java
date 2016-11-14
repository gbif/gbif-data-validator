package org.gbif.validation.collector;

import org.gbif.validation.api.model.DataFileDescriptor;
import static  org.gbif.validation.collector.DwcExtensionIntegrityValidation.collectUnlinkedExtensions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

public class DwcExtensionIntegrityValidationTest {

  private final static String RESOURCES_DIR = "dwc-data-integrity";

  /**
   * Tests that 2 extension columns are missing in the core file.
   */
  @Test
  public void collectUnlinkedExtensionsTest() throws IOException {
    String testDir = "missing2CoreIDs";
    List<String> unLinked  = collectUnlinkedExtensions(getCoreTestFileDescriptor(testDir), 0, //core, column
                                                       getExtensionTestFileDescriptor(testDir), 0, //ext, column
                                                       2); //# of samples
    Assert.assertTrue("2 extensions are not linked", unLinked.size() == 2);
  }

  /**
   * Tests that correct number of samples are collected.
   */
  @Test
  public void collectUnlinkedExtensionsSamplingTest() throws IOException {
    String testDir = "missing2CoreIDs";
    List<String> unLinked  = collectUnlinkedExtensions(getCoreTestFileDescriptor(testDir), 0, //core, column
                                                       getExtensionTestFileDescriptor(testDir), 0, //ext, column
                                                       1); //# of samples
    Assert.assertTrue("Only 1 sample was requested", unLinked.size() == 1);
  }

  /**
   * Tests that 2 extension columns are missing in columns different to the first columns in core and extension files.
   */
  @Test
  public void collectUnlinkedExtensionsNoInitialColumnsTest() throws IOException {
    String testDir = "integrityOn3rdColumn";
    List<String> unLinked  = collectUnlinkedExtensions(getCoreTestFileDescriptor(testDir), 2, //core, column
                                                       getExtensionTestFileDescriptor(testDir), 1, //ext, column
                                                       2); //# of samples
    Assert.assertTrue("2 extensions are not linked", unLinked.size() == 2);
  }


  private static DataFileDescriptor getCoreTestFileDescriptor(String testDir) {
     return getTestFileDescriptor(testDir, "core.txt");
  }

  private static DataFileDescriptor getExtensionTestFileDescriptor(String testDir) {
    return getTestFileDescriptor(testDir, "ext.txt");
  }

  /**
   * Utility class to create data file descriptors from tests files.
   */
  private static DataFileDescriptor getTestFileDescriptor(String testDir, String testFileName) {
    try {
      URL testFileUrl = Resources.getResource(Paths.get(RESOURCES_DIR, testDir, testFileName).toString());
      DataFileDescriptor dataFileDescriptor = new DataFileDescriptor();
      dataFileDescriptor.setFieldsTerminatedBy(',');
      dataFileDescriptor.setHasHeaders(true);
      dataFileDescriptor.setSubmittedFile(Paths.get(testFileUrl.toURI()).toString());
      return dataFileDescriptor;
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

}
