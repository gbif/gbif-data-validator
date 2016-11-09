package org.gbif.validation;

import org.gbif.utils.file.csv.CSVReaderFactory;
import org.gbif.utils.file.csv.UnkownDelimitersException;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResult;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class OccurrenceValidationApp {

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    DataFile dataFile = new DataFile();
    dataFile.setFilePath(Paths.get(fileName));
    dataFile.setSourceFileName(new File(fileName).toPath().getFileName().toString());
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setHasHeaders(true);
    //dataFile.setDelimiterChar('\t');

  //  extractAndSetTabularFileMetadata(Paths.get(fileName), dataFile);

    ResourceEvaluationManager resourceEvaluationManager = new ResourceEvaluationManager(args[1], 5);

    ValidationResult result = resourceEvaluationManager.evaluate(dataFile);

    ObjectMapper om = new ObjectMapper();
    om.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    System.out.println(om.writeValueAsString(result));
  }

  /**
   * TODO move to validator-processor
   * Method responsible to extract metadata (and headers) from the file identified by dataFilePath.
   *
   * @param dataFilePath location of the file
   * @param dataFile this object will be updated directly
   */
  private static void extractAndSetTabularFileMetadata(java.nio.file.Path dataFilePath, DataFile dataFile) {
    //TODO make use of CharsetDetection.detectEncoding(source, 16384);
    if(dataFile.getDelimiterChar() == null) {
      try {
        CSVReaderFactory.CSVMetadata metadata = CSVReaderFactory.extractCsvMetadata(dataFilePath.toFile(), "UTF-8");
        if (metadata.getDelimiter().length() == 1) {
          dataFile.setDelimiterChar(metadata.getDelimiter().charAt(0));
        } else {
          throw new UnkownDelimitersException(metadata.getDelimiter() + "{} is a non supported delimiter");
        }
      } catch (UnkownDelimitersException udEx) {
        udEx.printStackTrace();
      }
    }
  }
}
