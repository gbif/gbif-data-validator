package org.gbif.validation;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResult;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class OccurrenceValidationApp {

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    DataFile dataFile = new DataFile();
    dataFile.setFileName(fileName);
    dataFile.setSourceFileName(new File(fileName).toPath().getFileName().toString());
    dataFile.setFileFormat(FileFormat.DWCA);
    dataFile.setHasHeaders(true);

    ResourceEvaluationManager resourceEvaluationManager = new ResourceEvaluationManager(args[1]);

    ValidationResult result = resourceEvaluationManager.evaluate(dataFile);

    ObjectMapper om = new ObjectMapper();
    om.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    System.out.println(om.writeValueAsString(result));
  }
}
