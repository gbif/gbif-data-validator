package org.gbif.validation.evaluator;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link BasicMetadataEvaluator}.
 *
 */
public class BasicMetadataEvaluatorTest {

  private static final File DWC_ARCHIVE = FileUtils.getClasspathFile("dwca/dwca-eml-content-issue");

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * BasicMetadataEvaluator
   */
  @Test
  public void testBasicMetadataEvaluator() throws IOException, UnsupportedDataFileException {

    BasicMetadataEvaluator basicMetadataEvaluator = new BasicMetadataEvaluator();

    DataFile df = new DataFile(DWC_ARCHIVE.toPath(), "dwca-occurrence", FileFormat.DWCA, "");
    DwcDataFile dwcDf = DataFileFactory.prepareDataFile(df, folder.newFolder().toPath());

    Optional<List<ValidationResultElement>> result = basicMetadataEvaluator.evaluate(dwcDf);
    assertTrue(result.isPresent());
  }
}
