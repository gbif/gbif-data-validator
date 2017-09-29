package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.JobDataOutput;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.result.ValidationDataOutput;
import org.gbif.validation.jobserver.JobStorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * JobStorage implementation that stores and retrieves json files from a local file system.
 *
 * Status :
 *  {jobid}.json
 * Output data:
 *  {jobid}/{type}.json
 */
public class FileJobStorage implements JobStorage {

  //Jackson instances used to write and read Json files.
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  }

  private static final ObjectReader STATUS_OBJECT_READER = OBJECT_MAPPER.reader(JobStatusResponse.class);
  private static final ObjectWriter STATUS_OBJECT_WRITER = OBJECT_MAPPER.writerWithType(JobStatusResponse.class);

  private static final ObjectReader DATA_OUTPUT_OBJECT_READER = OBJECT_MAPPER.reader(JobDataOutput.class);
  private static final ObjectWriter DATA_OUTPUT_OBJECT_WRITER = OBJECT_MAPPER.writerWithType(JobDataOutput.class);

  private static final String JSON_EXT = ".json";

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
   * Checks that the directoryStorePath is a directory, if it doesn't exists it tries to create it.
   * An IllegalArgumentException is thrown if the directoryStorePath is not a directory or if it can't be created.
   */
  private static void createStorePath(Path directoryStorePath) {
    File storePathFile = directoryStorePath.toFile();
    Preconditions.checkArgument(storePathFile.isDirectory(), "Path is not a directory");
    if (!storePathFile.exists()){
      Preconditions.checkArgument(storePathFile.mkdirs(), "Store path can't be created");
    }
  }

  /**
   * Gets the path to the json file for a JobId.
   */
  private Path getJobResultFile(long jobId) {
    return storePath.resolve(Long.toString(jobId) + JSON_EXT);
  }

  /**
   * Get the {@link Path} of the output data file.
   * @param jobId
   * @param type
   * @return
   */
  private Path getJobOutputDataPath(long jobId, ValidationDataOutput.Type type) {
    return storePath.resolve(Long.toString(jobId)).resolve(type.name().toLowerCase() + JSON_EXT);
  }

  /**
   * Tries to get the Json file of a JobId.
   * Return and Optional.empty() is the file is not found.
   */
  @Override
  public Optional<JobStatusResponse<?>> getStatus(long jobId) throws IOException {
    Path jobFile = getJobResultFile(jobId);
    if (Files.exists(jobFile)) {
      return Optional.ofNullable(STATUS_OBJECT_READER.readValue(jobFile.toFile()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<JobDataOutput> getDataOutput(long jobId, ValidationDataOutput.Type type) throws IOException {
    File jobFile = getJobOutputDataPath(jobId, type).toFile();
    if (jobFile.exists()) {
      return Optional.ofNullable(DATA_OUTPUT_OBJECT_READER.readValue(jobFile));
    }
    return Optional.empty();
  }

  /**
   * Stores the data as Json file in a 'storePath/jobId.json' file.
   */
  @Override
  public void put(JobStatusResponse<?> response) {
    try {
      STATUS_OBJECT_WRITER.writeValue(getJobResultFile(response.getJobId()).toFile(), response);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Stores the data as Json file in a 'storePath/jobId/dataOutputType.json' file.
   */
  @Override
  public void put(JobDataOutput data) {
    Path outputDataFile = getJobOutputDataPath(data.getJobId(), data.getType());
    try {
      Files.createDirectories(outputDataFile.getParent());
      DATA_OUTPUT_OBJECT_WRITER.writeValue(outputDataFile.toFile(), data);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
