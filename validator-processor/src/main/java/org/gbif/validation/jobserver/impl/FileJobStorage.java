package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.jobserver.JobStorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;

/**
 * JobStorage implementation that stores and  retrieves json files from a local file system.
 */
public class FileJobStorage implements JobStorage {

  //Jackson instances used to write and read Json files.
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectReader OBJECT_READER = OBJECT_MAPPER.reader(JobStatusResponse.class);
  private static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writerWithType(JobStatusResponse.class);

  //Directory where the JSON files are stored.
  private final Path storePath;

  /**
   * Default constructor, uses the storePath as the directory where Json files are stored.
   */
  public FileJobStorage(Path storePath) {
    createStorePath(storePath);
    this.storePath = storePath;
  }

  /**
   * Checks that the storePath is a directory, if it doesn't exists it tries to create it.
   * An IllegalArgumentException is thrown if the storePath is not a directory or if it can't be created.
   */
  private static void createStorePath(Path storePath) {
    File storePathFile = storePath.toFile();
    Preconditions.checkArgument(storePathFile.isDirectory(), "Path is not a directory");
    if (!storePathFile.exists()){
      Preconditions.checkArgument(storePathFile.mkdirs(), "Store path can't be created");
    }
  }

  /**
   * Gets the path to the json file for a JobId.
   */
  private File getJobResultFile(long jobId) {
    return storePath.resolve(Long.toString(jobId) + ".json").toFile();
  }

  /**
   * Tries to get the Json file of a JobId.
   * Return and Optional.empty() is the file is not found.
   */
  @Override
  public Optional<JobStatusResponse<?>> get(long jobId) {
    try {
      File jobFile = getJobResultFile(jobId);
      if (jobFile.exists()) {
        return Optional.ofNullable(OBJECT_READER.readValue(jobFile));
      }
      return Optional.empty();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Stores the data as Json file in a 'storePath/jobId.json' file.
   */
  @Override
  public void put(JobStatusResponse<?> data) {
    try {
      OBJECT_WRITER.writeValue(getJobResultFile(data.getJobId()), data);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
