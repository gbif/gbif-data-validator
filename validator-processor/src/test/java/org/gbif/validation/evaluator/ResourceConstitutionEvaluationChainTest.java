package org.gbif.validation.evaluator;

import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.gbif.validation.TestUtils.getFirstValidationIssue;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests related to {@link ResourceConstitutionEvaluationChain}.
 */
public class ResourceConstitutionEvaluationChainTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final String TEST_NO_ID_CSV_FILE_LOCATION = "tabular/no_id.csv";

  @Test
  public void testResourceConstitutionEvaluationChain() throws IOException, UnsupportedDataFileException {
    DataFile dataFile = TestUtils.getDataFile(TEST_NO_ID_CSV_FILE_LOCATION, "no-id", FileFormat.TABULAR);
    DwcDataFileSupplier transformer = () -> DataFileFactory.prepareDataFile(dataFile,folder.newFolder().toPath());

    ResourceConstitutionEvaluationChain evaluationChain =
      ResourceConstitutionEvaluationChain.Builder.using(dataFile, TestUtils.getEvaluatorFactory())
              .transformedBy(transformer)
              .evaluateDwcDataFile(TestUtils.getEvaluatorFactory().createPrerequisiteEvaluator())
            .build();

    Optional<List<ValidationResultElement>> result = evaluationChain.run();

    assertTrue(evaluationChain.evaluationStopped());
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.CORE_ROWTYPE_UNDETERMINED, getFirstValidationIssue(result.get()).getIssue());
  }

}
