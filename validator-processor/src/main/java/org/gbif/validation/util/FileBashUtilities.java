package org.gbif.validation.util;

import org.apache.commons.lang3.text.StrTokenizer;
import org.gbif.utils.text.LineComparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * This class contains functions that encapsulate Linux commands.
 */
public class FileBashUtilities {

  //command that appends a newline at the end of the file if not already there
  private static final String ENSURE_FILE_ENDS_NEWLINE_CMD = "sed -i '' -e '$a\\' %s";

  //command to find content in a specific column of a file
  //private static final String FIND_FILE_CMD = "awk -v column=%d -v value='%s' -F'%s' '$column == value {print FNR}' %s";

  // Find duplicate values in a column.
  // ASCII 31 is the record separator character, which is unlikely to be part of an identifier or value.
  private static final String US = String.valueOf((char)31);
  private static final String FIND_DUPLICATE_CMD = "awk -F'%s' 'BEGIN{OFS=\"\\037\";} {cur = $%d} prev == cur { print $%d, $%d } {prev = cur}' %s";

  // Find empty values in a column.
  private static final String FIND_NULL_VALUE_CMD = "awk -F'%s' 'BEGIN{OFS=\"\\037\";} $%d == \"\" { print NR, $0 }' %s";

  /**
   * This command can does a diff between 2 files on a specific column.
   * FNR==NR matches only the first file so we read all the cells and store them 'a[$%d];next'
   * If the cell of the second file is not in the stored array, print it.
   * The command does not require the files to be sorted but stores the column of the reference file in memory.
   * !($%d in a))&&(FNR>1)
   */
  private static final String DIFF_CMD = "awk -F'%s' 'FNR==NR{a[$%d];next}!($%d in a){print $%d}' %s %s";

  private static final String DIFF_CMD_SKIP_HEADER = "awk -F'%s' '(FNR>1)&&(FNR==NR){a[$%d];next}(!($%d in a))&&(FNR>1){print $%d}' %s %s";

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
   * Limitation: this method doesn't look for quoted values so, when used for splitting tabular files (e.g. csv), it is
   * possible for a record to be splitted over 2 files.
   * Example of limitation : 1,2,"3\n4",5\n will be counted as 2 lines.
   *
   * @return paths of all files produced by the command sorted by natural order of {@link String} to get
   * file.txtaa before file.txtbb.
   * Never null.
   */
  public static String[] splitFile(String filePath, int splitSize, String outputDir) throws IOException {
    File outDir = new File(outputDir);
    File inFile = new File(filePath);
    checkArgument(!outDir.exists() || outDir.isDirectory(), "Output directory is not a directory");
    checkArgument(!outDir.exists() || outDir.list().length == 0, "Output directory should be empty");

    checkArgument(inFile.exists(), "Input file doesn't exist");

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    executeSimpleCmd(String.format("split -l %s %s %s", Integer.toString(splitSize), filePath,
            Paths.get(outputDir, inFile.getName())));
    String[] outputContent = Optional.ofNullable(outDir.list()).orElse(new String[0]);
    //sort using the ascending, natural order of String to get file.txtaa before file.txtbb
    Arrays.sort(outputContent);

    return outputContent;
  }

  /**
   * Find duplicated values of a column from a file already sorted on that column.
   *
   * With quoted delimiters, this can't work (there could even be quoted line separators), so we revert to Java.
   *
   * @param filePath path to the file sorted on the column
   * @param keyColumn index of the column to report, e.g. line ids
   * @param valueColumn index of the column to find duplicate, starting at 1
   * @param separator
   * @return
   * @throws IOException
   */
  public static List<String[]> findDuplicates(String filePath, int keyColumn, int valueColumn,
                                              String separator, Character quote, Charset encoding) throws IOException {
    checkArgument(keyColumn > 0, "Indices are starting at 1");
    checkArgument(valueColumn > 0, "Indices are starting at 1");
    if (quote == null || valueColumn == 1) {
      return executeFieldCmd(String.format(FIND_DUPLICATE_CMD, separator, valueColumn, keyColumn, valueColumn, filePath));
    } else {
      // Can't use a simple GNU command if fields may be quoted.
      LineComparator lineComparator = new LineComparator(valueColumn-1, separator, quote);

      StrTokenizer tokenizer = new StrTokenizer();
      tokenizer.setEmptyTokenAsNull(true);
      tokenizer.setIgnoreEmptyTokens(false);
      if (quote != null) {
        tokenizer.setQuoteChar(quote);
      }
      tokenizer.setDelimiterString(separator);

      String previous = null;
      List<String[]> duplicates = new ArrayList<>();

      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), encoding))) {
        String line = br.readLine();
        while (line != null) {
          if (lineComparator.compare(previous, line) == 0) {
            tokenizer.reset(line);
            String[] parts = tokenizer.getTokenArray();
            duplicates.add(new String[]{parts[keyColumn-1], parts[valueColumn-1]});
          }
          previous = line;
          line = br.readLine();
        }
      }
      return duplicates;
    }
  }

  /**
   * Find empty values in column.
   *
   * @param filePath path to the file
   * @param valueColumn index of the column to find empty values, starting at 1
   * @param separator
   * @return
   * @throws IOException
   */
  public static List<String[]> findEmpty(String filePath, int valueColumn, String separator) throws IOException {
    checkArgument(valueColumn > 0, "Indices are starting at 1");
    return executeFieldCmd(String.format(FIND_NULL_VALUE_CMD, separator, valueColumn, filePath));
  }

  /**
   * TODO javadoc
   * @param refFilePath
   * @param inputFilePath
   * @param columnNumberReferenceFile
   * @param columnNumberInputFile
   * @param sep
   * @param skipHeaderLine
   * @return
   * @throws IOException
   */
  public static String[] diffOnColumns(String refFilePath, String inputFilePath,
                                       int columnNumberReferenceFile, int columnNumberInputFile, String sep,
                                       boolean skipHeaderLine) throws IOException {
    checkArgument(columnNumberReferenceFile > 0 && columnNumberInputFile > 0, "Indices are starting at 1");

    return executeSimpleCmd(String.format(skipHeaderLine ? DIFF_CMD_SKIP_HEADER : DIFF_CMD, sep,
            columnNumberReferenceFile, columnNumberInputFile, columnNumberInputFile, refFilePath, inputFilePath));
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
   * FIXME limit the number of result returned
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

  /**
   * Executes a bash command and collect its result in a string array.
   * FIXME limit the number of result returned
   */
  private static List<String[]> executeFieldCmd(String bashCmd) throws IOException {
    List<String[]> output = new ArrayList<>();
    String[] cmd = {"/bin/sh", "-c", bashCmd};
    Process process = Runtime.getRuntime().exec(cmd);
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = in.readLine()) != null) {
        output.add(line.split(US));
      }
      process.waitFor();
      return output;
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (process.isAlive()) {
        process.destroy();
      }
    }
  }
}
