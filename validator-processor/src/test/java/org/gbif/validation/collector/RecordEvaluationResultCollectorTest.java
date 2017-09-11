package org.gbif.validation.collector;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests related to {@link RecordEvaluationResultCollector}.
 */
public class RecordEvaluationResultCollectorTest {

  @Test
  public void testCollector() {
    RecordEvaluationResultCollector collector = new RecordEvaluationResultCollector(10, false);

    RecordEvaluationResult.Builder bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1l);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());
    assertEquals(1L, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
  }

  @Test
  public void testCollectorSamplingLimit() {
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, false), 2L, 1L);
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, true), 2L, 1L);

    // still expecting 1 as sample size considering the input data is the same.
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(2, false), 2L, 1L);
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(2, true), 2L, 1L);
  }

  /**
   * Test functions that add 2 RecordEvaluationResult of type {@code COLUMN_MISMATCH} with the same
   * expected and found data.
   * @param collector
   * @param expectedCount expected evaluation type count
   * @param expectedSampleSize expected size of the sample taken
   */
  private static void testCollectorSamplingLimit(RecordEvaluationResultCollector collector, long expectedCount,
                                                 long expectedSampleSize) {
    RecordEvaluationResult.Builder bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1L);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());

    bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 2L);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());

    assertEquals(expectedCount, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
    assertEquals(expectedSampleSize, collector.getSamples().get(EvaluationType.COLUMN_MISMATCH).size());
  }

}
