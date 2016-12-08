package org.gbif.validation.api.model;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.RecordEvaluator;

import java.util.Arrays;
import javax.annotation.Nullable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests related to {@link RecordEvaluatorChain}.
 */
public class RecordEvaluatorChainTest {

  @Test
  public void testRecordEvaluatorChain() {
    MyRecordEvaluator1 myRecordEvaluator1 = new MyRecordEvaluator1();
    MyRecordEvaluator2 myRecordEvaluator2 = new MyRecordEvaluator2();

    RecordEvaluator recordEvaluatorChain = new RecordEvaluatorChain(Arrays.asList(myRecordEvaluator1, myRecordEvaluator2));

    RecordEvaluationResult result = recordEvaluatorChain.evaluate(1l, new String[0]);

    assertNotNull(result);
    //the order should be preserved MyRecordEvaluator1 first
    assertEquals("MyRecordEvaluator1", ((RecordEvaluationResult.BaseEvaluationResultDetails) result.getDetails().get(0)).getExpected());
    // MyRecordEvaluator2 second
    assertEquals("MyRecordEvaluator2", ((RecordEvaluationResult.BaseEvaluationResultDetails) result.getDetails().get(1)).getExpected());
  }

  private static class MyRecordEvaluator1 implements RecordEvaluator {
    @Nullable
    @Override
    public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {
      return RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, lineNumber).addBaseDetail(
              EvaluationType.COLUMN_MISMATCH, "MyRecordEvaluator1", "MyRecordEvaluator1").build();
    }
  }

  private static class MyRecordEvaluator2 implements RecordEvaluator {
    @Nullable
    @Override
    public RecordEvaluationResult evaluate(@Nullable Long lineNumber, @Nullable String[] record) {
      return RecordEvaluationResult.Builder.of(DwcTerm.Occurrence, lineNumber).addBaseDetail(
              EvaluationType.COLUMN_MISMATCH, "MyRecordEvaluator2", "MyRecordEvaluator2").build();
    }
  }

}
