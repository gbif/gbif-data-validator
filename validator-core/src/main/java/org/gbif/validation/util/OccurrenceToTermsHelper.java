package org.gbif.validation.util;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.api.model.occurrence.Occurrence;

import java.util.HashMap;
import java.util.Map;

/**
 * FIXME this class should NOT be here
 */
public class OccurrenceToTermsHelper {

  /**
   * We only have this method to avoid using reflexion (for performance reason)
   * @param occurrence
   * @return
   */
  public static Map<Term, Object> getTermsMap(Occurrence occurrence) {

    Map<Term, Object> fields = new HashMap<>();

    fields.put(DwcTerm.eventDate, occurrence.getEventDate());
    fields.put(DwcTerm.year, occurrence.getYear());
    fields.put(DwcTerm.month, occurrence.getMonth());
    fields.put(DwcTerm.day, occurrence.getDay());
    fields.put(DwcTerm.decimalLatitude, occurrence.getDecimalLatitude());
    fields.put(DwcTerm.decimalLongitude, occurrence.getDecimalLongitude());
    fields.put(GbifTerm.taxonKey, occurrence.getTaxonKey());

    return fields;
  }

}
