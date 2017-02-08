package org.gbif.validation.processor;

import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.source.DataFileFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import static org.gbif.validation.TestUtils.getDwcaDataFile;

/**
 * Unit tests related to {@link EvaluationChain}.
 */
public class EvaluationChainTest {

  @Test
  public void test() {

  //  private static File testChecklistFile = FileUtils.getClasspathFile("checklists/00000001-c6af-11e2-9b88-00145eb45e9a");

    DataFile dwcaDataFile = getDwcaDataFile("dwca-megachile_chomskyi-v12.2", "test");
    try {
      List<TabularDataFile> dataFiles = DataFileFactory.prepareDataFile(dwcaDataFile);
      EvaluationChain.Builder evaluationChainBuilder = EvaluationChain.Builder.using(dwcaDataFile, dataFiles,
              TestUtils.getEvaluatorFactory());

      evaluationChainBuilder.evaluateReferentialIntegrity();

      evaluationChainBuilder.build().runRowTypeEvaluation(rowTypeEvaluationUnit -> {
        try {
          Optional<Stream<RecordEvaluationResult>> result = rowTypeEvaluationUnit.evaluate();
          result.ifPresent( r -> r.forEach(System.out::println));
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

    } catch (IOException e) {
      org.junit.Assert.fail(e.getMessage());
    }


  }

}
