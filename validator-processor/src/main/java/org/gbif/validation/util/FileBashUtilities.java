package org.gbif.validation.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileBashUtilities {

  /**
   * Private constructor.
   */
  private FileBashUtilities() {
    //empty constructor
  }

  /**
   * Counts the number of lines in a text file.
   */
  public static int countLines(String fileName) throws IOException {
    String[] out = executeSimpleCmd(String.format("wc -l %s | awk '{print $1;}'", fileName));
    return Integer.parseInt(out[0]);
  }

  /**
   * Split a text file into pieces of size 'splitSize'.
   */
  public static String[] splitFile(String fileName, int splitSize, String outputDir) throws IOException {
    File outDir = new File(outputDir);
    File inFile = new File(fileName);
    checkArgument((outDir.exists() && outDir.isDirectory()) || !outDir.exists(), "Output path is not a directory");
    checkArgument((outDir.exists() && outDir.list().length == 0) || !outDir.exists(),
                  "Output directory should be empty");
    checkArgument(inFile.exists(), "Input file doesn't exist");

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    executeSimpleCmd(String.format("split -l %s %s %s", Integer.toString(splitSize), fileName,
                                   Paths.get(outputDir, inFile.getName())));
    return outDir.list();
  }

  /**
   * Applies the command 'awk -v column=column -v value='value' -F'\t' '$column == value {print FNR}' fileName'.
   * It returns the lines number where the pattern occurs.
   */
  public static Integer[] findInFile(String fileName, String value, int column, String separator) throws IOException {
    String[] lines =  executeSimpleCmd(String.format("awk -v column=%d -v value='%s' -F'%s' '$column == value {print FNR}' %s",
                                                     column,value,separator,fileName));
    return Arrays.stream(lines).map(lineNr -> Integer.parseInt(lineNr)).toArray(size -> new Integer[size]);
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
   */
  private static String[] executeSimpleCmd(String bashCmd) throws  IOException {
    String[] cmd = { "/bin/sh", "-c", bashCmd };
    Process process = Runtime.getRuntime().exec(cmd);
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      List<String> out = new ArrayList<String>();
      while ((line = in.readLine()) != null) {
        out.add(line);
      }
      while (process.isAlive()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {

        }
      }
      return out.toArray(new String[out.size()]);
    } finally {
      if (process.isAlive()) {
        process.destroy();
      }
    }
  }
}
