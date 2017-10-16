package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.gbif.validation.TestUtils.getDwcaDataFile;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests related to {@link EvaluationChain}.
 */
public class EvaluationChainTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private DataFile dataFile = getDwcaDataFile("dwca/dwca-ref-integrity-issue", "test");

  @Test
  public void testBasicEvaluationChain() throws UnsupportedDataFileException {
    try {
      DwcDataFileSupplier transformer = () -> DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());

      //DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dwcaDataFile, testFolder);
      EvaluationChain.Builder evaluationChainBuilder = EvaluationChain.Builder.using(dataFile,
              transformer, TestUtils.getEvaluatorFactory(), folder.newFolder().toPath());
      evaluationChainBuilder.evaluateReferentialIntegrity();

      EvaluationChain ec = evaluationChainBuilder.build();
      ResourceConstitutionEvaluationChain.ResourceConstitutionResult resourceConstitutionResult = ec.runResourceConstitutionEvaluation();
      assertFalse(resourceConstitutionResult.isEvaluationStopped());
      ec.runRecordCollectionEvaluation((dataFile, rowType, recordCollectionEvaluator) -> {
        try {
          List<RecordEvaluationResult> results = new ArrayList<>();
          recordCollectionEvaluator.evaluate(dataFile, results::add);
          if(DwcTerm.Identification.equals(rowType)){
            assertTrue("Got referential integrity issue on Identification extensions", !results.isEmpty());
          }
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
