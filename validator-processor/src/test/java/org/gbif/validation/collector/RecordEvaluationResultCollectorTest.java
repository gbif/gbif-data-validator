package org.gbif.validation.collector;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.result.ValidationResultDetails;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests related to {@link RecordEvaluationResultCollector}.
 */
public class RecordEvaluationResultCollectorTest {

  @Test
  public void testCollector() {
    RecordEvaluationResultCollector collector = new RecordEvaluationResultCollector(10, false);

    RecordEvaluationResult.Builder bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, 1L);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, "2", "3");
    collector.collect(bldr.build());
    assertEquals(1L, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
  }

  @Test
  public void testCollectorSamplingLimit() {
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, false), 1L);
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(1, true), 1L);

    // expecting 2 as sample size even if the input data is the same.
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(2, false), 2L);
    testCollectorSamplingLimit(new RecordEvaluationResultCollector(2, true), 2L);
  }

  @Test
  public void testCollectorSamplingContent() {
    testCollectorSamplingContent(new RecordEvaluationResultCollector(1, false), 1L);
    testCollectorSamplingContent(new RecordEvaluationResultCollector(1, true), 1L);

    testCollectorSamplingContent(new RecordEvaluationResultCollector(2, false), 1L, 3L);
    testCollectorSamplingContent(new RecordEvaluationResultCollector(2, true), 1L, 3L);
  }

  public static RecordEvaluationResult buildRecordEvaluationResult(long id, String expected, String found) {
    RecordEvaluationResult.Builder bldr = RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, id);
    bldr.addBaseDetail(EvaluationType.COLUMN_MISMATCH, expected, found);
    return bldr.build();
  }

  public void testCollectorSamplingContent(RecordEvaluationResultCollector collector, Long ... lineNumber) {
    collector.collect(buildRecordEvaluationResult(1L, "2", "3"));
    collector.collect(buildRecordEvaluationResult(2L, "2", "3"));
    collector.collect(buildRecordEvaluationResult(3L, "3", "4"));

    assertEquals(3, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
    assertEquals(lineNumber.length, collector.getSamples().get(EvaluationType.COLUMN_MISMATCH).size());

    int numberFound = 0;
    for(Long lNum : lineNumber) {
      for(ValidationResultDetails details : collector.getSamples().get(EvaluationType.COLUMN_MISMATCH)){
        if(lNum.equals(details.getLineNumber())){
          numberFound++;
        }
      }
    }
    assertEquals(lineNumber.length, numberFound);
  }

  /**
   * Test functions that add 2 RecordEvaluationResult of type {@code COLUMN_MISMATCH} with the same
   * expected and found data.
   * @param collector
   * @param expectedSampleSize expected size of the sample taken
   */
  private static void testCollectorSamplingLimit(RecordEvaluationResultCollector collector, long expectedSampleSize) {
    collector.collect(buildRecordEvaluationResult(1L, "2", "3"));
    collector.collect(buildRecordEvaluationResult(2L, "2", "3"));
    assertEquals(2, collector.getAggregatedCounts().get(EvaluationType.COLUMN_MISMATCH).longValue());
    assertEquals(expectedSampleSize, collector.getSamples().get(EvaluationType.COLUMN_MISMATCH).size());
  }

}
