package org.gbif.validation.evaluator;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.TestUtils;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import static org.gbif.validation.TestUtils.XML_CATALOG;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basis tests for {@link DwcaResourceStructureEvaluator}
 */
public class DwcaResourceStructureEvaluatorTest {

  private static final DwcaResourceStructureEvaluator DWCA_RESOURCES_STRUCTURE_EVAL =
          new DwcaResourceStructureEvaluator(new XMLSchemaValidatorProvider(Optional.of(XML_CATALOG.getAbsolutePath())),
                  TestUtils.EXTENSION_MANAGER);

  /**
   * Get a DataFile instance for a file in the classpath.
   * Also used by EmlResourceStructureEvaluatorTest.
   * @param resourcePath
   * @param sourceFileName
   * @return
   */
  static DataFile getDataFile(String resourcePath, String sourceFileName) {
    File dwcaFolder = FileUtils.getClasspathFile(resourcePath);
    return new DataFile(dwcaFolder.toPath(), sourceFileName, FileFormat.DWCA, "");
  }

  /**
   * This test is slow (~5 sec)
   */
  @Test
  public void dwcaResourceStructureEvaluatorExtTest() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-invalid-ext", "test"));
    assertTrue(result.isPresent());
    assertTrue(result.get().get(0).getIssues().stream()
            .filter(vi -> EvaluationType.REQUIRED_TERM_MISSING.equals(vi.getIssue()))
            .findFirst().isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTest() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence", "test"));
    assertFalse(result.isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestBrokenMetaXml() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence-broken", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_UNREADABLE, result.get().get(0).getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestMetaXmlSchema() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence-schema", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_SCHEMA, result.get().get(0).getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestNoMetaXml() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence-no-meta", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_NOT_FOUND, result.get().get(0).getIssues().get(0).getIssue());
  }

}
