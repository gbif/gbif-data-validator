package org.gbif.validation.evaluator;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 *
 */
public class ChecklistValidatorTest {

  private static File testChecklistFile = FileUtils.getClasspathFile("checklists/00000001-c6af-11e2-9b88-00145eb45e9a");
  private static File normalizerConfigFile = FileUtils.getClasspathFile("checklists/clb-normalizer.yaml");

//  @Test
//  public void testChecklistEvaluator() {
//    NormalizerConfiguration config = getNormalizerConfiguration();
//    config.archiveRepository = FileUtils.getClasspathFile("checklists");
//    ChecklistEvaluator checklistEvaluator = new ChecklistEvaluator(config);
//
//    DataFile testChecklistDataFile = new DataFile(testChecklistFile.toPath(), "00000001-c6af-11e2-9b88-00145eb45e9a",
//            FileFormat.DWCA, "");
//
//    try {
//      Optional<Stream<RecordEvaluationResult>> a = checklistEvaluator.evaluate(testChecklistDataFile);
//      a.get().forEach(System.out::println);
//    } catch (IOException e) {
//      fail(e.getMessage());
//    }
//  }

  private static NormalizerConfiguration getNormalizerConfiguration() {
    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(normalizerConfigFile.toURI().toURL(),
              NormalizerConfiguration.class);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
