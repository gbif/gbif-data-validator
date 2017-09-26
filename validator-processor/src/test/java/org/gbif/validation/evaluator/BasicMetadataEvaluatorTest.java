package org.gbif.validation.evaluator;

import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationDataOutput;
import org.gbif.validation.api.result.ValidationIssue;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link BasicMetadataEvaluator}.
 *
 */
public class BasicMetadataEvaluatorTest {

  private static final String DWC_ARCHIVE = "dwca/dwca-eml-content-issue";
  private static final String DWC_ARCHIVE_NO_ISSUE = "dwca/dwca-occurrence";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * BasicMetadataEvaluator
   */
  @Test
  public void testBasicMetadataEvaluator() throws IOException, UnsupportedDataFileException {

    BasicMetadataEvaluator basicMetadataEvaluator = new BasicMetadataEvaluator();

    DataFile df = TestUtils.getDwcaDataFile(DWC_ARCHIVE, "dwca-eml-content-issue");
    DwcDataFile dwcDf = DataFileFactory.prepareDataFile(df, folder.newFolder().toPath());

    Optional<List<ValidationResultElement>> result = basicMetadataEvaluator.evaluate(dwcDf);
    assertTrue(result.isPresent());

    List<ValidationResultElement> validationResultElements = result.get();
    assertEquals(1, validationResultElements.size());
    List<ValidationIssue> issues = validationResultElements.get(0).getIssues();
    assertEquals(4, issues.size());

    //should have those issue
    List<EvaluationType> issueTypes = issues.stream().map(issue -> issue.getIssue()).collect(Collectors.toList());
    assertTrue(issueTypes.contains(EvaluationType.LICENSE_MISSING_OR_UNKNOWN));
    assertTrue(issueTypes.contains(EvaluationType.TITLE_MISSING_OR_TOO_SHORT));
    assertTrue(issueTypes.contains(EvaluationType.DESCRIPTION_MISSING_OR_TOO_SHORT));
    assertTrue(issueTypes.contains(EvaluationType.RESOURCE_CONTACTS_MISSING_OR_INCOMPLETE));
  }

  /**
   * BasicMetadataEvaluator
   */
  @Test
  public void testBasicMetadataEvaluatorNoIssue() throws IOException, UnsupportedDataFileException {
    BasicMetadataEvaluator basicMetadataEvaluator = new BasicMetadataEvaluator();
    DataFile df = TestUtils.getDwcaDataFile(DWC_ARCHIVE_NO_ISSUE, "dwca-occurrence");
    DwcDataFile dwcDf = DataFileFactory.prepareDataFile(df, folder.newFolder().toPath());

    Optional<List<ValidationResultElement>> result = basicMetadataEvaluator.evaluate(dwcDf);
    assertTrue(result.isPresent());

    assertEquals(1, result.get().get(0).getDataOutput().size());
    assertEquals(ValidationDataOutput.Type.DATASET_OBJECT, result.get().get(0).getDataOutput().get(0).getType());
  }
}
