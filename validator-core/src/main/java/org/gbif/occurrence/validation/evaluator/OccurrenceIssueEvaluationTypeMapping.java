package org.gbif.occurrence.validation.evaluator;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.model.EvaluationType;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Basically a 1-to-1 mapping between {@link EvaluationType} and supported {@link OccurrenceIssue}.
 * Unit test will validate that we have a mapping for all supported {@link OccurrenceIssue}.
 */
public class OccurrenceIssueEvaluationTypeMapping {

  public static final Map<OccurrenceIssue, EvaluationType> OCCURRENCE_ISSUE_MAPPING =
          Collections.unmodifiableMap(Stream.of(
                  new SimpleEntry<>(OccurrenceIssue.ZERO_COORDINATE, EvaluationType.ZERO_COORDINATE),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_OUT_OF_RANGE, EvaluationType.COORDINATE_OUT_OF_RANGE),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_INVALID, EvaluationType.COORDINATE_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_ROUNDED, EvaluationType.COORDINATE_ROUNDED),
                  new SimpleEntry<>(OccurrenceIssue.GEODETIC_DATUM_INVALID, EvaluationType.GEODETIC_DATUM_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84, EvaluationType.GEODETIC_DATUM_ASSUMED_WGS84),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_REPROJECTED, EvaluationType.COORDINATE_REPROJECTED),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_REPROJECTION_FAILED, EvaluationType.COORDINATE_REPROJECTION_FAILED),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS, EvaluationType.COORDINATE_REPROJECTION_SUSPICIOUS),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_PRECISION_INVALID, EvaluationType.COORDINATE_PRECISION_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID, EvaluationType.COORDINATE_UNCERTAINTY_METERS_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, EvaluationType.COUNTRY_COORDINATE_MISMATCH),
                  new SimpleEntry<>(OccurrenceIssue.COUNTRY_MISMATCH, EvaluationType.COUNTRY_MISMATCH),
                  new SimpleEntry<>(OccurrenceIssue.COUNTRY_INVALID, EvaluationType.COUNTRY_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES, EvaluationType.COUNTRY_DERIVED_FROM_COORDINATES),
                  new SimpleEntry<>(OccurrenceIssue.CONTINENT_COUNTRY_MISMATCH, EvaluationType.CONTINENT_COUNTRY_MISMATCH),
                  new SimpleEntry<>(OccurrenceIssue.CONTINENT_INVALID, EvaluationType.CONTINENT_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.CONTINENT_DERIVED_FROM_COORDINATES, EvaluationType.CONTINENT_DERIVED_FROM_COORDINATES),
                  new SimpleEntry<>(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, EvaluationType.PRESUMED_SWAPPED_COORDINATE),
                  new SimpleEntry<>(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE, EvaluationType.PRESUMED_NEGATED_LONGITUDE),
                  new SimpleEntry<>(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, EvaluationType.PRESUMED_NEGATED_LATITUDE),
                  new SimpleEntry<>(OccurrenceIssue.RECORDED_DATE_MISMATCH, EvaluationType.RECORDED_DATE_MISMATCH),
                  new SimpleEntry<>(OccurrenceIssue.RECORDED_DATE_INVALID, EvaluationType.RECORDED_DATE_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.RECORDED_DATE_UNLIKELY, EvaluationType.RECORDED_DATE_UNLIKELY),
                  new SimpleEntry<>(OccurrenceIssue.TAXON_MATCH_FUZZY, EvaluationType.TAXON_MATCH_FUZZY),
                  new SimpleEntry<>(OccurrenceIssue.TAXON_MATCH_HIGHERRANK, EvaluationType.TAXON_MATCH_HIGHERRANK),
                  new SimpleEntry<>(OccurrenceIssue.TAXON_MATCH_NONE, EvaluationType.TAXON_MATCH_NONE),
                  new SimpleEntry<>(OccurrenceIssue.DEPTH_NOT_METRIC, EvaluationType.DEPTH_NOT_METRIC),
                  new SimpleEntry<>(OccurrenceIssue.DEPTH_UNLIKELY, EvaluationType.DEPTH_UNLIKELY),
                  new SimpleEntry<>(OccurrenceIssue.DEPTH_MIN_MAX_SWAPPED, EvaluationType.DEPTH_MIN_MAX_SWAPPED),
                  new SimpleEntry<>(OccurrenceIssue.DEPTH_NON_NUMERIC, EvaluationType.DEPTH_NON_NUMERIC),
                  new SimpleEntry<>(OccurrenceIssue.ELEVATION_UNLIKELY, EvaluationType.ELEVATION_UNLIKELY),
                  new SimpleEntry<>(OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED, EvaluationType.ELEVATION_MIN_MAX_SWAPPED),
                  new SimpleEntry<>(OccurrenceIssue.ELEVATION_NOT_METRIC, EvaluationType.ELEVATION_NOT_METRIC),
                  new SimpleEntry<>(OccurrenceIssue.ELEVATION_NON_NUMERIC, EvaluationType.ELEVATION_NON_NUMERIC),
                  new SimpleEntry<>(OccurrenceIssue.MODIFIED_DATE_INVALID, EvaluationType.MODIFIED_DATE_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, EvaluationType.MODIFIED_DATE_UNLIKELY),
                  new SimpleEntry<>(OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, EvaluationType.IDENTIFIED_DATE_UNLIKELY),
                  new SimpleEntry<>(OccurrenceIssue.IDENTIFIED_DATE_INVALID, EvaluationType.IDENTIFIED_DATE_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.BASIS_OF_RECORD_INVALID, EvaluationType.BASIS_OF_RECORD_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.TYPE_STATUS_INVALID, EvaluationType.TYPE_STATUS_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.MULTIMEDIA_DATE_INVALID, EvaluationType.MULTIMEDIA_DATE_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.MULTIMEDIA_URI_INVALID, EvaluationType.MULTIMEDIA_URI_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.REFERENCES_URI_INVALID, EvaluationType.REFERENCES_URI_INVALID),
                  new SimpleEntry<>(OccurrenceIssue.INTERPRETATION_ERROR, EvaluationType.INTERPRETATION_ERROR),
                  new SimpleEntry<>(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, EvaluationType.INDIVIDUAL_COUNT_INVALID))
                  .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));
}
