package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.gbif.validation.TestUtils.getDwcaDataFile;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

/**
 * Unit tests related to {@link EvaluationChain}.
 */
public class EvaluationChainTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private DataFile dwcaDataFile = getDwcaDataFile("dwca/dwca-ref-integrity-issue", "test");

  @Test
  public void testBasicEvaluationChain() throws UnsupportedDataFileException {
    try {
      Path testFolder = folder.newFolder().toPath();
      DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dwcaDataFile, testFolder);
      EvaluationChain.Builder evaluationChainBuilder = EvaluationChain.Builder.using(dwcDataFile,
              TestUtils.getEvaluatorFactory(), testFolder);

      evaluationChainBuilder.evaluateReferentialIntegrity();
      evaluationChainBuilder.build().runRecordCollectionEvaluation((dataFile, rowType, recordCollectionEvaluator) -> {
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
