package org.gbif.validation.api.model;

/**
 * Enumeration of all possible types for an evaluation.
 * OccurrenceIssue are copied here to access all evaluation types by a single enum.
 * Mapping is done by OccurrenceIssueEvaluationTypeMapping and covered by unit test.
 */
public enum EvaluationType {

  RECORD_NOT_UNIQUELY_IDENTIFIED(EvaluationCategory.RESOURCE_STRUCTURE),

  COLUMN_MISMATCH(EvaluationCategory.RECORD_STRUCTURE),

  //occurrence record completeness
  GEOSPATIAL_DATA_NOT_PROVIDED(EvaluationCategory.OCCURRENCE_RECORD_STRUCTURE),
  TEMPORAL_DATA_NOT_PROVIDED(EvaluationCategory.OCCURRENCE_RECORD_STRUCTURE),
  TAXONOMIC_DATA_NOT_PROVIDED(EvaluationCategory.OCCURRENCE_RECORD_STRUCTURE),

  ZERO_COORDINATE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_OUT_OF_RANGE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_ROUNDED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  GEODETIC_DATUM_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  GEODETIC_DATUM_ASSUMED_WGS84(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTION_FAILED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTION_SUSPICIOUS(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_PRECISION_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_UNCERTAINTY_METERS_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_COORDINATE_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_DERIVED_FROM_COORDINATES(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_COUNTRY_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_DERIVED_FROM_COORDINATES(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_SWAPPED_COORDINATE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_NEGATED_LONGITUDE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_NEGATED_LATITUDE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_FUZZY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_HIGHERRANK(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_NONE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_NOT_METRIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_MIN_MAX_SWAPPED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_NON_NUMERIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_MIN_MAX_SWAPPED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_NOT_METRIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_NON_NUMERIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MODIFIED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MODIFIED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  IDENTIFIED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  IDENTIFIED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  BASIS_OF_RECORD_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TYPE_STATUS_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MULTIMEDIA_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MULTIMEDIA_URI_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  REFERENCES_URI_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  INTERPRETATION_ERROR(EvaluationCategory.OCC_INTERPRETATION_BASED),
  INDIVIDUAL_COUNT_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED);

  private final EvaluationCategory category;

  EvaluationType(EvaluationCategory category) {
    this.category = category;
  }

  public EvaluationCategory getCategory() {
    return category;
  }

}
