package org.gbif.validation.util;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Unit tests related to {@link FileNormalizer}.
 */
public class FileNormalizerTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final File TEST_FOLDER = FileUtils.getClasspathFile("normalizer/subfolder");
  private static final File LATIN_CRLF_TEST_FILE = FileUtils.getClasspathFile("normalizer/latin1_crlf.txt");
  private static final File UTF8_CR_TEST_FILE = FileUtils.getClasspathFile("normalizer/utf8_cr.txt");
  private static final File UTF8_LF_TEST_FILE = FileUtils.getClasspathFile("normalizer/utf8_lf.txt");

  @Test
  public void testLatinCRLF() throws IOException {
    testNormalizer(LATIN_CRLF_TEST_FILE.toPath(), StandardCharsets.ISO_8859_1);
  }

  @Test
  public void testUtf8CR() throws IOException {
    testNormalizer(UTF8_CR_TEST_FILE.toPath(), StandardCharsets.UTF_8);
  }

  @Test
  public void testUtf8LF() throws IOException {
    testNormalizer(UTF8_LF_TEST_FILE.toPath(), StandardCharsets.UTF_8);
  }

  @Test
  public void normalizeTarget() throws IOException {
    File normalizedFolder = folder.newFolder();
    Map<Path, Charset> charsets = new HashMap<>();
    Map<Path, Integer> normalizedContent = FileNormalizer.normalizeTarget(TEST_FOLDER.toPath(),
            normalizedFolder.toPath(), charsets);
    assertEquals(2, normalizedContent.size());
    assertEquals(3, normalizedContent.get(Paths.get("utf8_lf.txt")).intValue());
    assertEquals(2, normalizedContent.get(Paths.get("subsubfolder/utf8_lf.txt")).intValue());
  }

  private void testNormalizer(Path testFile, Charset charset) throws IOException {
    File normalizedFile = folder.newFile();
    long numberOfLine = FileNormalizer.normalizeFile(testFile, normalizedFile.toPath(), charset);

    assertEquals(3, numberOfLine);

    // Use the Apache FileUtils to get the entire content as a String
    String fileContent = org.apache.commons.io.FileUtils.readFileToString(normalizedFile, "UTF-8");

    assertFalse(fileContent.contains("\r"));
    assertTrue(fileContent.contains("é"));
  }

  /**
   * normalizeTarget should also work for a single file
   */
  @Test
  public void testNormalizerUsingTarget() throws IOException {
    File normalizedFolder = folder.newFolder();
    Map<Path, Charset> charsets = new HashMap<>();
    charsets.put(Paths.get(LATIN_CRLF_TEST_FILE.getName()), StandardCharsets.ISO_8859_1);

    //test without sending the encoding so the default (UTF-8) is used
    Map<Path, Integer> normalizedContent = FileNormalizer.normalizeTarget(LATIN_CRLF_TEST_FILE.toPath(),
            normalizedFolder.toPath(), null);
    assertEquals(1, normalizedContent.size());
    // 0 is expected
    assertEquals(0, normalizedContent.get(Paths.get(LATIN_CRLF_TEST_FILE.getName())).intValue());

    //test sending the right encoding
    normalizedContent = FileNormalizer.normalizeTarget(LATIN_CRLF_TEST_FILE.toPath(),
            normalizedFolder.toPath(), charsets);
    assertEquals(1, normalizedContent.size());
    assertEquals(3, normalizedContent.get(Paths.get(LATIN_CRLF_TEST_FILE.getName())).intValue());
  }

}
