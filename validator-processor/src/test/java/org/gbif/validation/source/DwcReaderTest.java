package org.gbif.validation.source;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.RecordSource;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests related to
 */
public class DwcReaderTest {

  private static final String TEST_FILE_LOCATION = "dwc-archive";

  @Test
  public void testDwcReader() {

    RecordSource source = null;
    File testFolder = FileUtils.getClasspathFile(TEST_FILE_LOCATION);
    try {
      source = RecordSourceFactory.fromDwcA(testFolder);
      assertEquals(13, source.getHeaders().length);

      String[] line1 = source.read();
      assertEquals("1559060", line1[0]);
      assertEquals("my-default-value", line1[12]);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    finally {
      IOUtils.closeQuietly(source);
    }
  }

}
