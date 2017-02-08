package org.gbif.validation.evaluator;

import org.gbif.validation.TestUtils;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.util.List;
import java.util.Optional;

import org.junit.Test;

import static org.gbif.validation.TestUtils.XML_CATALOG;
import static org.gbif.validation.TestUtils.getDwcaDataFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basis tests for {@link DwcaResourceStructureEvaluator}.
 *
 */
public class DwcaResourceStructureEvaluatorTest {

  private static final DwcaResourceStructureEvaluator DWCA_RESOURCES_STRUCTURE_EVAL =
          new DwcaResourceStructureEvaluator(new XMLSchemaValidatorProvider(Optional.of(XML_CATALOG.getAbsolutePath())),
                  TestUtils.EXTENSION_MANAGER);

  /**
   * This test is slow (~5 sec)
   */
  @Test
  public void dwcaResourceStructureEvaluatorExtTest() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDwcaDataFile("dwca/dwca-invalid-ext", "test"));
    assertTrue(result.isPresent());
    assertTrue(result.get().get(0).getIssues().stream()
            .filter(vi -> EvaluationType.REQUIRED_TERM_MISSING.equals(vi.getIssue()))
            .findFirst().isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTest() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDwcaDataFile("dwca/dwca-occurrence", "test"));
    assertFalse(result.isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestBrokenMetaXml() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDwcaDataFile("dwca/dwca-occurrence-broken", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_UNREADABLE, result.get().get(0).getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestMetaXmlSchema() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDwcaDataFile("dwca/dwca-occurrence-schema", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_SCHEMA, result.get().get(0).getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestNoMetaXml() {
    Optional<List<ValidationResultElement>> result =
            DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(getDwcaDataFile("dwca/dwca-occurrence-no-meta", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_NOT_FOUND, result.get().get(0).getIssues().get(0).getIssue());
  }

}
