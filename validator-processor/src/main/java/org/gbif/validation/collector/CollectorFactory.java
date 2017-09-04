package org.gbif.validation.collector;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.ResultsCollector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;

/**
 * Factory that know how to build the different collectors.
 */
public class CollectorFactory {

  private static final List<Term> OCCURRENCE_INTERPRETED_TERMS = Collections.unmodifiableList(
          Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey));

  private static final List<Term> TAXON_INTERPRETED_TERMS = Collections.unmodifiableList(
          Arrays.asList(DwcTerm.kingdom, DwcTerm.family, DwcTerm.taxonRank));

  /**
   * Create a {@link ResultsCollector} to collected interpreted terms if applicable.
   * Interpreted terms are only available for a limited set of rowType where we apply interpretation in
   * the evaluation process.
   *
   * We return the concrete type to let the caller use implementation specific methods.
   *
   * @param rowType
   * @param useConcurrentMap
   * @return Optional InterpretedTermsCountCollector instance.
   */
  public static Optional<InterpretedTermsCountCollector> createInterpretedTermsCountCollector(@NotNull Term rowType,
                                                                                boolean useConcurrentMap) {
    Objects.requireNonNull(rowType, "rowType shall be provided");
    if(DwcTerm.Occurrence == rowType) {
      return Optional.of(new InterpretedTermsCountCollector(OCCURRENCE_INTERPRETED_TERMS, useConcurrentMap));
    }

    if(DwcTerm.Taxon == rowType) {
      return Optional.of(new InterpretedTermsCountCollector(TAXON_INTERPRETED_TERMS, useConcurrentMap));
    }

    return Optional.empty();
  }


}
