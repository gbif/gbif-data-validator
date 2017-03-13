package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

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
      DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dwcaDataFile, folder.newFolder().toPath());
      EvaluationChain.Builder evaluationChainBuilder = EvaluationChain.Builder.using(dwcDataFile,
              TestUtils.getEvaluatorFactory());

      evaluationChainBuilder.evaluateReferentialIntegrity();
      evaluationChainBuilder.build().runRowTypeEvaluation((dataFile, rowType, recordCollectionEvaluator) -> {
        try {
          Optional<Stream<RecordEvaluationResult>> result = recordCollectionEvaluator.evaluate(dataFile);
          if(DwcTerm.Identification.equals(rowType)){
            assertTrue("Got referential integrity issue on Identification extensions", result.isPresent());
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
