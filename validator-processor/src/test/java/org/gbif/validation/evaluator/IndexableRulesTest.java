package org.gbif.validation.evaluator;

import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;

import org.junit.Test;

import static org.gbif.validation.TestUtils.mockMetadataValidationResultElementList;

import static org.junit.Assert.assertFalse;

/**
 * Unit tests related to {@link IndexableRules}
 */
public class IndexableRulesTest {
  
  @Test
  public void testIsIndexable() {
    List<ValidationResultElement> validationResultElementList = mockMetadataValidationResultElementList(EvaluationType.LICENSE_MISSING_OR_UNKNOWN);
    assertFalse(IndexableRules.isIndexable(validationResultElementList));
  }
}
