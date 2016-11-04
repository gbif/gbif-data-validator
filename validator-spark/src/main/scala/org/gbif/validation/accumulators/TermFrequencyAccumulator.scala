package org.gbif.validation.accumulators

import org.apache.spark.AccumulatorParam
import org.gbif.dwc.terms.Term

import org.gbif.validation.conversion.MapConversions._


/**
  * Accumulates terms frequencies.
  */
class TermFrequencyAccumulator extends AccumulatorParam[Map[Term, Long]]  {

  override def addInPlace(r1: Map[Term, Long], r2: Map[Term, Long]): Map[Term, Long] =
   return r1.accumulate(r2)

  override def zero(initialValue: Map[Term, Long]): Map[Term, Long] =  initialValue

}
