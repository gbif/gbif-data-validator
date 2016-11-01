package org.gbif.validation;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationResult;
import org.gbif.validation.tabular.DataFileProcessorFactory;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class OccurrenceValidationApp {

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    DataFile dataFile = new DataFile();
    dataFile.setFileName(fileName);
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setHasHeaders(true);
    DataFileProcessorFactory dataFileProcessorFactory = new DataFileProcessorFactory(args[1]);
    DataFileProcessor dataFileProcessor = dataFileProcessorFactory.create(dataFile);
    ValidationResult result = dataFileProcessor.process(dataFile);

    ObjectMapper om = new ObjectMapper();
    om.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    System.out.println(om.writeValueAsString(result));
  }
}
