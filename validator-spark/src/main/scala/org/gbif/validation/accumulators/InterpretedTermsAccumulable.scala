package org.gbif.validation.accumulators

import org.apache.spark.AccumulableParam
import org.gbif.dwc.terms.Term
import org.gbif.validation.api.model.RecordEvaluationResult
import org.gbif.validation.conversion.UtilConversions._

import scala.collection.JavaConversions._

/**
  * Accumulates counts of interpreted terms.
  */
class InterpretedTermsAccumulable extends AccumulableParam[Map[Term, Long], RecordEvaluationResult]  {

  override def addAccumulator(r: Map[Term, Long], t: RecordEvaluationResult): Map[Term, Long] = {
    if (t.getInterpretedData == null) r else r.accumulate(t.getInterpretedData.map({case (term,value) => (term, if (value == null) 0L else 1L)}).toMap)
  }

  override def addInPlace(r1: Map[Term, Long], r2: Map[Term, Long]): Map[Term, Long] =
    r1.accumulate(r2)

  override def zero(initialValue: Map[Term, Long]): Map[Term, Long] = initialValue
}
