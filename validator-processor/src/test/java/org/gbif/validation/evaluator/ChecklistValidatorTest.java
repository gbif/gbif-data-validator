package org.gbif.validation.evaluator;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 *
 */
public class ChecklistValidatorTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private DataFile checklistDataFile = org.gbif.validation.TestUtils.getDataFile(
          "checklists/00000001-c6af-11e2-9b88-00145eb45e9a", "test", FileFormat.TABULAR);

  @Test
  public void testChecklistEvaluator() throws IOException, UnsupportedDataFileException {
    ChecklistEvaluator checklistEvaluator = new ChecklistEvaluator(new NormalizerConfiguration(),
            folder.newFolder().toPath());

    Path testFolder = folder.newFolder().toPath();
    DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(checklistDataFile, testFolder);

    InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(Collections.singletonList(DwcTerm.taxonRank), false);

    try {
      Optional<Stream<RecordEvaluationResult>> a = checklistEvaluator.evaluate(dwcDataFile);
      a.get().forEach(interpretedTermsCountCollector::collect);
      assertEquals(Long.valueOf(20), interpretedTermsCountCollector.getInterpretedCounts().get(DwcTerm.taxonRank));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

}
