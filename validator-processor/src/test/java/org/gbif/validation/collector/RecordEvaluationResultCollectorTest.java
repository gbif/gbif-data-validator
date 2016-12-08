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
    assertEquals(1l, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
  }

  @Test
  public void testCollectorSamplingLimit(){
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, false));
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, true));
  }

  /**
   *
   * @param collector expected to have maxNumberOfSample == 1
   */
  private static void testCollectorSamplingLimit(RecordEvaluationResultCollector collector) {
    RecordEvaluationResult.Builder bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1l);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());

    bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 2l);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());

    //count should be 2
    assertEquals(2l, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
    //sampling should be 1
    assertEquals(1l, collector.getSamples().get(EvaluationType.COLUMN_MISMATCH).size());
  }
}
