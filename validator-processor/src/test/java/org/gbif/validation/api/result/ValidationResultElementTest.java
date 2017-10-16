package org.gbif.validation.api.result;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.vocabulary.DwcFileType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests related to {@link ValidationResultElement}.
 *
 */
public class ValidationResultElementTest {

  @Test
  public void testMergeOnFilename() {
    List<ValidationResultElement> source = new ArrayList<>();
    List<ValidationResultElement> mergeInto = new ArrayList<>();

    source.add(ValidationResultElement.forMetadata("test.txt", Collections.singletonList(
            ValidationIssues.withEvaluationTypeOnly(EvaluationType.LICENSE_MISSING_OR_UNKNOWN)), null));

    mergeInto.add(new ValidationResultElement("test.txt", 18L, DwcFileType.CORE, DwcTerm.Occurrence,
            DwcTerm.occurrenceID, Lists.newArrayList(ValidationIssues.withSample(EvaluationType.INDIVIDUAL_COUNT_INVALID, 1,
            Collections.emptyList())), null));

    mergeInto.add(new ValidationResultElement("test2.txt", 18L, DwcFileType.CORE, DwcTerm.Occurrence,
            DwcTerm.occurrenceID, Lists.newArrayList(ValidationIssues.withSample(EvaluationType.INDIVIDUAL_COUNT_INVALID, 1,
            Collections.emptyList())), null));

    ValidationResultElement.mergeOnFilename(source, mergeInto);
    assertEquals(2, mergeInto.size());
    ValidationResultElement testTxtElement = mergeInto.get(0);
    assertEquals("test.txt", testTxtElement.getFileName());
    assertEquals(2, testTxtElement.getIssues().size());
    //assert that the LICENSE_MISSING_OR_UNKNOWN is now attached to "test.txt" in the mergeInto collection
    assertTrue(testTxtElement.getIssues().stream()
            .filter( issue -> EvaluationType.LICENSE_MISSING_OR_UNKNOWN == issue.getIssue())
            .findFirst()
            .isPresent());
  }
}
