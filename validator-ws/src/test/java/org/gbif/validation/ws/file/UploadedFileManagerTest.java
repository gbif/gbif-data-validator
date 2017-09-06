package org.gbif.validation.ws.file;


import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.compress.archivers.ArchiveException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for UploadedFileManager
 */
public class UploadedFileManagerTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @Test
  public void testUnzipWithFolders() {
    try {
      File testFolder = folder.newFolder("subfolder");
      File f = FileUtils.getClasspathFile("zip/zip-test-with-folder.zip");
      FileInputStream fis = new FileInputStream(f);
      try {
        UploadedFileManager.unzip(fis, testFolder.toPath());
        assertZipFolderContent(testFolder);
      } catch (ArchiveException e) {
        e.printStackTrace();
        fail();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testDetermineDataFilePath() {
    try {
      File testFolder = folder.newFolder("subfolder2");
      File f = FileUtils.getClasspathFile("zip/zip-test-with-root-folder.zip");
      FileInputStream fis = new FileInputStream(f);
      try {
        UploadedFileManager.unzip(fis, testFolder.toPath());
        //determineDataFilePath allows to ignore root folder
        assertZipFolderContent(UploadedFileManager.determineDataFilePath(testFolder.toPath()).toFile());
      } catch (ArchiveException e) {
        e.printStackTrace();
        fail();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  private void assertZipFolderContent(File extractFolder) {
    File[] unzippedFiles = extractFolder.listFiles(pathname -> !pathname.isDirectory());
    File[] unzippedFolder = extractFolder.listFiles(pathname -> pathname.isDirectory());
    assertEquals(1, unzippedFiles.length);
    assertEquals(1, unzippedFolder.length);

    assertEquals("file_A.txt", unzippedFiles[0].getName());
    assertEquals("my-folder", unzippedFolder[0].getName());
    assertEquals("file_B.txt", unzippedFolder[0].listFiles()[0].getName());
  }

  @Test
  public void testParseContentDisposition() {
    assertEquals("validator_test_file_all_issues.tsv",
            UploadedFileManager.parseContentDisposition("form-data; name=\"file\"; filename=\"validator_test_file_all_issues.tsv\"").get());
    assertEquals("validator_test_file_all_issues.tsv",
            UploadedFileManager.parseContentDisposition("form-data; name=\"file\"; fileName =\"validator_test_file_all_issues.tsv\"").get());
    assertEquals("validator_test_file_all_issues.tsv",
            UploadedFileManager.parseContentDisposition("form-data; name=\"file\"; filename = validator_test_file_all_issues.tsv").get());

    assertFalse(UploadedFileManager.parseContentDisposition("").isPresent());
    assertFalse(UploadedFileManager.parseContentDisposition("form-data; name=\"file\";").isPresent());
    assertFalse(UploadedFileManager.parseContentDisposition("name=\"file\"; filename=").isPresent());
  }

}
