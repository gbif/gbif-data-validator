package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Class responsible to handle the logic related to the file splitting strategy.
 */
class DataFileSplitter {

  /**
   * Split the provided {@link TabularDataFile} into multiple {@link TabularDataFile} if required.
   * If no split is required the returning list will contain the provided {@link TabularDataFile}.
   *
   * @param dataFile expected to have at least the followings: rowType, filePath, numOfLines, hasHeaders
   * @param fileSplitSize
   * @param baseDir       Base directory where to store the results. A folder like "Occurrence_split" will be created.
   *
   * @return
   *
   * @throws IOException
   */
  static List<TabularDataFile> splitDataFile(TabularDataFile dataFile, Integer fileSplitSize, Path baseDir) throws IOException {
    Objects.requireNonNull(dataFile.getRowTypeKey(), "DataFile getRowTypeKey shall be provided");
    Objects.requireNonNull(dataFile.getFilePath(), "DataFile filePath shall be provided");

    List<TabularDataFile> splitDataFiles = new ArrayList<>();
    if (dataFile.getNumOfLines() <= fileSplitSize) {
      splitDataFiles.add(dataFile);
    } else {
      String splitFolder = baseDir.resolve(dataFile.getRowTypeKey().name() + "_split").toAbsolutePath().toString();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFilePath().toString(), fileSplitSize, splitFolder);

      boolean inputHasHeaders = dataFile.isHasHeaders();
      //number of lines in the last file
      int remainingLines =  dataFile.getNumOfLines() - ((splits.length - 1) * fileSplitSize);
      IntStream.range(0, splits.length)
              .forEach(idx -> splitDataFiles.add(newSplitDataFile(dataFile,
                      splitFolder,
                      splits[idx],
                      inputHasHeaders && (idx == 0),
                      (idx * fileSplitSize),
                      idx < splits.length -1 ? fileSplitSize : remainingLines)));
    }
    return splitDataFiles;
  }

  /**
   * Get a new {@link DataFile} instance representing a split of the provided {@link DataFile}.
   *
   * @param dataFile
   * @param baseDir
   * @param fileName
   * @param withHeader
   * @param offset
   *
   * @return new {@link DataFile} representing a portion of the provided dataFile.
   */
  private static TabularDataFile newSplitDataFile(TabularDataFile dataFile, String baseDir, String fileName,
                                           boolean withHeader, Integer offset, Integer numberOfLines) {
    //Creates the file to be used
    File splitFile = new File(baseDir, fileName);
    splitFile.deleteOnExit();

    return DataFileFactory.newTabularDataFileSplit(dataFile, Paths.get(splitFile.getAbsolutePath()),
            offset, numberOfLines, numberOfLines - (withHeader ? 1 : 0) , withHeader);
  }
}
