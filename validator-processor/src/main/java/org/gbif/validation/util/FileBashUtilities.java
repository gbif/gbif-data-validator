package org.gbif.validation.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.Validate;

/**
 * This class contains functions that encapsulate Linux commands.
 */
public class FileBashUtilities {

  //command that appends a newline at the end of the file if not already there
  private static final String ENSURE_FILE_ENDS_NEWLINE_CMD = "sed -i '' -e '$a\\' %s";

  //command to find content in a specific column of a file
  private static final String FIND_FILE_CMD = "awk -v column=%d -v value='%s' -F'%s' '$column == value {print FNR}' %s";
  private static final String FIND_DUPLICATE_CMD = "awk -F'%s' '{cur = $%d} prev == cur { print $%d} {prev = cur}' %s";

  /**
   * Private constructor.
   */
  private FileBashUtilities() {
    //empty constructor
  }

  /**
   * Ensures that the provided file ends with a newline.
   * The provided file will be modified to add a newline if not already present.
   * Mostly used before using {@link #countLines(String)} to ensure the last line in counted.
   * @param filePath
   * @throws IOException
   */
  public static void ensureEndsWithNewline(String filePath) throws IOException {
    File inFile = new File(filePath);
    checkArgument(inFile.exists(), "Input file doesn't exist");

    executeSimpleCmd(String.format(ENSURE_FILE_ENDS_NEWLINE_CMD, filePath));
  }

  /**
   * Counts the number of lines in a text file.
   * Note: the last line will not be counted if it doesn't end with a newline. Use {@link #ensureEndsWithNewline(String)}
   */
  public static int countLines(String filePath) throws IOException {
    File inFile = new File(filePath);
    checkArgument(inFile.exists(), "Input file doesn't exist");

    //wc will not count the last line if it doesn't end with an endline
    String[] out = executeSimpleCmd(String.format("wc -l %s | awk '{print $1;}'", filePath));
    return Integer.parseInt(out[0]);
  }

  /**
   * Split a text file into pieces of size 'splitSize'.
   */
  public static String[] splitFile(String filePath, int splitSize, String outputDir) throws IOException {
    File outDir = new File(outputDir);
    File inFile = new File(filePath);
    checkArgument((outDir.exists() && outDir.isDirectory()) || !outDir.exists(), "Output path is not a directory");
    checkArgument((outDir.exists() && outDir.list().length == 0) || !outDir.exists(),
                  "Output directory should be empty");
    checkArgument(inFile.exists(), "Input file doesn't exist");

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    executeSimpleCmd(String.format("split -l %s %s %s", Integer.toString(splitSize), filePath,
                                   Paths.get(outputDir, inFile.getName())));
    return outDir.list();
  }

  /**
   * Applies the command 'awk -v column=column -v value='value' -F'\t' '$column == value {print FNR}' fileName'.
   * It returns the lines number where the pattern occurs.
   */
  public static Integer[] findInFile(String fileName, String value, int column, String separator) throws IOException {
    String[] lines =  executeSimpleCmd(String.format(FIND_FILE_CMD, column, value, separator, fileName));
    return Arrays.stream(lines).map(Integer::parseInt).toArray(Integer[]::new);
  }

  /**
   * Find duplicated values of a column from a file already sorted on that column.
   *
   * @param filePath path to the file sorted on the column
   * @param column index of the column to find duplicate, starting at 1
   * @param separator
   * @return
   * @throws IOException
   */
  public static String[] findDuplicates(String filePath, int column, String separator) throws IOException {
    Validate.isTrue(column > 0, "Indices are starting at 1");
    return executeSimpleCmd(String.format(FIND_DUPLICATE_CMD, separator, column, column, filePath));
  }

  /**
   * Utility method to validate arguments.
   */
  private static void checkArgument(Boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Executes a bash command and collect its result in a string array.
   * FIXME limit the number of result return
   */
  private static String[] executeSimpleCmd(String bashCmd) throws IOException {
    String[] cmd = {"/bin/sh", "-c", bashCmd};
    Process process = Runtime.getRuntime().exec(cmd);
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      List<String> out = new ArrayList<>();
      while ((line = in.readLine()) != null) {
        out.add(line);
      }
      process.waitFor();
      return out.toArray(new String[out.size()]);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (process.isAlive()) {
        process.destroy();
      }
    }
  }
}
