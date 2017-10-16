package org.gbif.validation.evaluator;

import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.Collections;

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
    DwcDataFileSupplier transformer = () -> DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());

    ResourceConstitutionEvaluationChain evaluationChain = new ResourceConstitutionEvaluationChain(dataFile,
            Collections.singletonList(TestUtils.getEvaluatorFactory().createResourceStructureEvaluator(dataFile.getFileFormat())),
            transformer, Collections.singletonList(TestUtils.getEvaluatorFactory().createPrerequisiteEvaluator()));

    ResourceConstitutionEvaluationChain.ResourceConstitutionResult result = evaluationChain.run();

    assertTrue(result.isEvaluationStopped());
    assertEquals(EvaluationType.CORE_ROWTYPE_UNDETERMINED, getFirstValidationIssue(result.getResults()).getIssue());
  }

}
