package org.gbif.validation.accumulators

import org.apache.spark.AccumulableParam
import org.gbif.validation.api.model.EvaluationType
import org.gbif.validation.api.model.RecordEvaluationResult
import org.gbif.validation.conversion.MapConversions._
import scala.collection.JavaConversions._

class ResultsAccumulable extends AccumulableParam[Map[EvaluationType, Long], RecordEvaluationResult]  {

  override def addAccumulator(r: Map[EvaluationType, Long], t: RecordEvaluationResult): Map[EvaluationType, Long] = {
    if (t.getDetails == null) r else r.accumulate(t.getDetails.map(details => (details.getEvaluationType, 1L)).toMap)
  }

  override def addInPlace(r1: Map[EvaluationType, Long], r2: Map[EvaluationType, Long]): Map[EvaluationType, Long] =
    r1.accumulate(r2)

  override def zero(initialValue: Map[EvaluationType, Long]): Map[EvaluationType, Long] = initialValue
}
