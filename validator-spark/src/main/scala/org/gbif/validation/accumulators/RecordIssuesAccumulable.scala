package org.gbif.validation.accumulators

import org.apache.spark.AccumulableParam
import org.gbif.validation.api.model.{EvaluationType, RecordEvaluationResult}
import org.gbif.validation.api.result.EvaluationResultDetails

import scala.collection.JavaConversions._

/**
  * Accumulates counts of interpreted terms.
  */
class RecordIssuesAccumulable(val maxNumberOfSample: Integer) extends AccumulableParam[Map[EvaluationType, List[EvaluationResultDetails]], RecordEvaluationResult]  {
  val DEFAULT_MAX_NUMBER_OF_SAMPLE = 10;

  override def addAccumulator(r: Map[EvaluationType, List[EvaluationResultDetails]],
    t: RecordEvaluationResult): Map[EvaluationType, List[EvaluationResultDetails]] =
    if (t.getDetails == null) r else r ++ t.getDetails.map(detail => { val evalType = r.get(detail.getEvaluationType);
      if(evalType.isDefined && r.size < maxNumberOfSample) (evalType,List(detail))}).toMap

  override def addInPlace(r1: Map[EvaluationType, List[EvaluationResultDetails]],
    r2: Map[EvaluationType, List[EvaluationResultDetails]]): Map[EvaluationType, List[EvaluationResultDetails]] =
    r1 ++ r2

  override def zero(
    initialValue: Map[EvaluationType, List[EvaluationResultDetails]]): Map[EvaluationType, List[EvaluationResultDetails]] = initialValue
}
