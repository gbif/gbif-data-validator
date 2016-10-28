package org.gbif.validation.api.model;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.model.ValidationResult.RecordsValidationResourceResultBuilder;

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
            ValidationResult.Builder.of(true, "myfile.zip", FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE)
                    .withResourceResult(RecordsValidationResourceResultBuilder.of("myfile.csv", 18l)
                            .withTermsFrequency(termsFreq).build())
                    .withResourceResult(RecordsValidationResourceResultBuilder.of("myextfile.csv", 4l)
                            .withInterpretedValueCounts(interpretedValuesCount).build())
                    .build();

    assertEquals(new Long(18l), ((ValidationResult.RecordsValidationResourceResult)result.getResults().get(0))
            .getTermsFrequency().get(DwcTerm.scientificName));
    assertEquals(new Long(4l), ((ValidationResult.RecordsValidationResourceResult) result.getResults().get(1))
            .getInterpretedValueCounts().get(DwcTerm.scientificName));
  }
}
