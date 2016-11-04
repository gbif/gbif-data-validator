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

  private DwcaResourceStructureEvaluator dwcaResourceStructureEvaluator = new DwcaResourceStructureEvaluator(new XMLSchemaValidatorProvider());

  @Test
  public void dwcaResourceStructureEvaluatorTest() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence");
    Optional<ValidationResultElement> result = dwcaResourceStructureEvaluator.evaluate(dwcaFolder.toPath(), "test");
    assertFalse(result.isPresent());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestBrokenMetaXml() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence-broken");
    Optional<ValidationResultElement> result = dwcaResourceStructureEvaluator.evaluate(dwcaFolder.toPath(), "test");
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_UNREADABLE, result.get().getIssues().get(0).getIssue());
  }

  @Test
  public void dwcaResourceStructureEvaluatorTestMetaXmlSchame() {
    File dwcaFolder = FileUtils.getClasspathFile("dwca-occurrence-schema");
    Optional<ValidationResultElement> result = dwcaResourceStructureEvaluator.evaluate(dwcaFolder.toPath(), "test");
    assertTrue(result.isPresent());
    assertEquals(EvaluationType.DWCA_META_XML_SCHEMA, result.get().getIssues().get(0).getIssue());
  }

}
