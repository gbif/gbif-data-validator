package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.JobDataOutput;
import org.gbif.validation.api.result.ValidationDataOutput;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;

/**
 * unit tests related to {@link FileJobStorage}.
 */
public class FileJobStorageTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testFileJobStorage() throws IOException {
    FileJobStorage fs = new FileJobStorage(folder.newFolder().toPath());
    JobDataOutput jdo = new JobDataOutput(123456L, ValidationDataOutput.Type.DATASET_OBJECT, "test");
    fs.put(jdo);

    JobDataOutput loadedJdo = fs.getDataOutput(123456L, ValidationDataOutput.Type.DATASET_OBJECT).get();

    assertEquals(jdo.getContent(), loadedJdo.getContent());
  }
}
