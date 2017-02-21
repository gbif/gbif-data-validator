package org.gbif.validation.util;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Unit tests for {@link FileBashUtilities}.
 */
public class FileBashUtilitiesTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final File SPLITTER_TEST_FILE = FileUtils.getClasspathFile("splitter/original_file.csv");
  private static final File SPLITTER_TEST_FILE_NONEWLINE = FileUtils.getClasspathFile("splitter/original_file_no_newline.csv");

  private static final Path DIFF_TEST_FOLDER = FileUtils.getClasspathFile("dwc-data-integrity").toPath();

  @Test
  public void testCountLines() throws IOException {
    //won't work on Windows
    assumeTrue(SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC);

    File t1 = copyTestFile(SPLITTER_TEST_FILE, "count_t1.csv");
    File t2 = copyTestFile(SPLITTER_TEST_FILE_NONEWLINE, "count_t2.csv");

    assertEquals(5, FileBashUtilities.countLines(t1.getAbsolutePath()));
    //test the assumption that "wc" will not count the last line of it doesn't end with a newline
    assertEquals(4, FileBashUtilities.countLines(t2.getAbsolutePath()));

    //fix the newline
    FileBashUtilities.ensureEndsWithNewline(t2.getAbsolutePath());
    assertEquals(5, FileBashUtilities.countLines(t2.getAbsolutePath()));

    //fix the newline again to make sure we do not create a new line
    FileBashUtilities.ensureEndsWithNewline(t2.getAbsolutePath());
    assertEquals(5, FileBashUtilities.countLines(t2.getAbsolutePath()));
  }

  @Test
  public void testSplit() throws IOException {
    //won't work on Windows
    assumeTrue(SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC);

    File t1 = copyTestFile(SPLITTER_TEST_FILE, "split_t1.csv");
    File t2 = copyTestFile(SPLITTER_TEST_FILE_NONEWLINE, "split_nonewline_t2.csv");

    //try first with the file that includes a newline
    assertEquals(3, splitInNewFolder(t1, 2));

    //then, with the file that does NOT include a newline
    //NOTE: on Mac version of 'split' (based on BSD) the last line will be ignored but on GNU based version, it won't
    int numberOfSplitFile = splitInNewFolder(t2, 2);
    assertTrue(SystemUtils.IS_OS_MAC && numberOfSplitFile == 2 ||
            SystemUtils.IS_OS_LINUX && numberOfSplitFile == 3);

    //In all cases, ensure we can "fix" the newline with no side effects
    FileBashUtilities.ensureEndsWithNewline(t2.getAbsolutePath());
    assertEquals(3, splitInNewFolder(t2, 2));
  }

  /**
   * Utility method to simply run the diffOnColumns command and check the number of results.
   */
  private static void runIntegrityTest(String referenceFilePath, String inputFilePath,
                                         int referenceFileCol, int inputFileColumn, int expectedCount,
                                       boolean skipHeaders) throws IOException {
    String[] output = FileBashUtilities.diffOnColumns(
            DIFF_TEST_FOLDER.resolve(referenceFilePath).toString(),
            DIFF_TEST_FOLDER.resolve(inputFilePath).toString(),
            referenceFileCol, inputFileColumn, ",", skipHeaders);
    Assert.assertEquals(expectedCount, output.length);
  }

  /**
   * Tests that 2 extension columns are missing in the core file.
   */
  @Test
  public void testDiffOnColumns2Missing() throws IOException {
    runIntegrityTest("missing2CoreIDs/core.txt", "missing2CoreIDs/ext.txt", 1, 1, 2, true);
  }

  /**
   * Tests that 2 extension columns are missing in columns different to the first columns in core and extension files.
   */
  @Test
  public void testDiffOnColumnsOtherColumns() throws IOException {
    runIntegrityTest("integrityOn3rdColumn/core.txt", "integrityOn3rdColumn/ext.txt", 3, 2, 2, false);
  }

  /**
   * Tests integrity check within the same file.
   * Currently it doesn't accept an empty reference that is often used to identify the root in Taxon file.
   */
  @Test
  public void testDiffOnColumnsOnSameFile() throws IOException {
    runIntegrityTest("integrityInSameFile/core.txt", "integrityInSameFile/core.txt", 1, 5, 1, true);
  }

  /**
   * We need to copy files since we will modify them
   *
   * @param testFile
   * @param fileName
   *
   * @return
   *
   * @throws IOException
   */
  private File copyTestFile(File testFile, String fileName) throws IOException {
    File testFileCopy = folder.newFile(fileName);
    org.apache.commons.io.FileUtils.copyFile(testFile, testFileCopy);
    return testFileCopy;
  }

  private int splitInNewFolder(File sourceFile, int splitSize) throws IOException {
    File destFolder = folder.newFolder();
    FileBashUtilities.splitFile(sourceFile.getAbsolutePath(), splitSize, destFolder.getAbsolutePath());
    List<File> splitFiles = Arrays.asList(destFolder.listFiles());
    return splitFiles.size();
  }

//  private void assertSplitInNewFolder(File sourceFile, int splitSize, int expectedNumberOfSplitFile) throws IOException {
//    File destFolder = folder.newFolder();
//    FileBashUtilities.splitFile(sourceFile.getAbsolutePath(), splitSize, destFolder.getAbsolutePath());
//    List<File> splitFiles = Arrays.asList(destFolder.listFiles());
//    assertEquals(expectedNumberOfSplitFile, splitFiles.size());
//  }

}
