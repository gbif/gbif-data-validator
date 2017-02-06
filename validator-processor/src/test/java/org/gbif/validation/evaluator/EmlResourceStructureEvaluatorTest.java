package org.gbif.validation.evaluator;

import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.util.List;
import java.util.Optional;

import org.junit.Test;

import static org.gbif.validation.TestUtils.XML_CATALOG;
import static org.gbif.validation.evaluator.DwcaResourceStructureEvaluatorTest.getDataFile;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basis tests for {@link EmlResourceStructureEvaluator}
 */
public class EmlResourceStructureEvaluatorTest {

  private static final EmlResourceStructureEvaluator EML_RESOURCES_STRUCTURE_EVAL =
          new EmlResourceStructureEvaluator(new XMLSchemaValidatorProvider(Optional.of(XML_CATALOG.getAbsolutePath())));

  @Test
  public void emlResourceStructureEvaluatorTest() {
    Optional<List<ValidationResultElement>> result =
            EML_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence", "test"));
    assertFalse(result.isPresent());
  }

  @Test
  public void emlResourceStructureEvaluatorTestBrokenEml() {
    Optional<List<ValidationResultElement>> result =
            EML_RESOURCES_STRUCTURE_EVAL.evaluate(getDataFile("dwca/dwca-occurrence-eml-broken", "test"));
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.EML_GBIF_SCHEMA, result.get().get(0).getIssues().get(0).getIssue());
  }
}
