package org.gbif.validation.api.model;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Tests related to {@link ValidationResult}.
 */
public class ValidationResultTest {

  @Test
  public void testFluentBuilder() {
    Map<Term, Long> termsFreq = new HashMap<>();
    termsFreq.put(DwcTerm.scientificName, 18l);

    Map<Term, Long> interpretedValuesCount = new HashMap<>();
    interpretedValuesCount.put(DwcTerm.scientificName, 4l);

    ValidationResult result =
            ValidationResultBuilders.Builder.of(true, "myfile.zip", FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE)
                    .withResourceResult(ValidationResultBuilders.RecordsValidationResultElementBuilder.of("myfile.csv",
                            DwcTerm.Occurrence, 18l)
                            .withTermsFrequency(termsFreq).build())
                    .withResourceResult(ValidationResultBuilders.RecordsValidationResultElementBuilder.of("myextfile.csv",
                            DwcTerm.Occurrence, 4l)
                            .withInterpretedValueCounts(interpretedValuesCount).build())
                    .build();

    assertEquals(new Long(18l), ((RecordsValidationResultElement) result.getResults().get(0))
            .getTermsFrequency().get(DwcTerm.scientificName));
    assertEquals(new Long(4l), ((RecordsValidationResultElement) result.getResults().get(1))
            .getInterpretedValueCounts().get(DwcTerm.scientificName));
  }
}
