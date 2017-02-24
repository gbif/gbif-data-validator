package org.gbif.validation.util;

import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

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

  private void testNormalizer(Path testFile, Charset charset) throws IOException {
    File normalizedFile = folder.newFile();
    long numberOfLine = FileNormalizer.normalizeFile(testFile, normalizedFile.toPath(),
            Optional.of(charset));

    assertEquals(3l, numberOfLine);

    // Use the Apache FileUtils to get the entire content as a String
    String fileContent = org.apache.commons.io.FileUtils.readFileToString(normalizedFile, "UTF-8");

    assertFalse(fileContent.contains("\r"));
    assertTrue(fileContent.contains("é"));
  }

}
