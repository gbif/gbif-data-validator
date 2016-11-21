package org.gbif.validation.evaluator;

import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;

import java.io.File;
import java.util.Optional;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basis tests for {@link DwcaResourceStructureEvaluator}
 */
public class DwcaResourceStructureEvaluatorTest {

  private static final File XML_CATALOG = FileUtils.getClasspathFile("xml/xml-catalog.xml");
  private DwcaResourceStructureEvaluator DWCA_RESOURCES_STRUCTURE_EVAL =
          new DwcaResourceStructureEvaluator(new XMLSchemaValidatorProvider(Optional.of(XML_CATALOG.getAbsolutePath())));

  @Test
  public void dwcaResourceStructureEvaluatorTest() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence");
    Optional<ValidationResultElement> result = DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(dwcaFolder.toPath(), "test");
    assertFalse(result.isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestBrokenMetaXml() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence-broken");
    Optional<ValidationResultElement> result = DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(dwcaFolder.toPath(), "test");
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_UNREADABLE, result.get().getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestMetaXmlSchema() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence-schema");
    Optional<ValidationResultElement> result = DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(dwcaFolder.toPath(), "test");
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_SCHEMA, result.get().getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestNoMetaXml() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence-no-meta");
    Optional<ValidationResultElement> result = DWCA_RESOURCES_STRUCTURE_EVAL.evaluate(dwcaFolder.toPath(), "test");
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_NOT_FOUND, result.get().getIssues().get(0).getIssue());
  }

}
