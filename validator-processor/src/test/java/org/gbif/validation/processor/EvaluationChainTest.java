package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
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

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

/**
 * Unit tests related to {@link EvaluationChain}.
 */
public class EvaluationChainTest {

  private DataFile dwcaDataFile = getDwcaDataFile("dwca/dwca-ref-integrity-issue", "test");

  @Test
  public void testBasicEvaluationChain() {
    try {
      List<TabularDataFile> dataFiles = DataFileFactory.prepareDataFile(dwcaDataFile);
      EvaluationChain.Builder evaluationChainBuilder = EvaluationChain.Builder.using(dwcaDataFile, dataFiles,
              TestUtils.getEvaluatorFactory());

      evaluationChainBuilder.evaluateReferentialIntegrity();
      evaluationChainBuilder.build().runRowTypeEvaluation(rowTypeEvaluationUnit -> {
        try {
          Optional<Stream<RecordEvaluationResult>> result = rowTypeEvaluationUnit.evaluate();
          if(DwcTerm.Identification.equals(rowTypeEvaluationUnit.getRowType())){
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
