package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;

import java.util.Optional;
import java.util.stream.Stream;

import akka.actor.AbstractLoggingActor;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Simple actor that receives a {@link DataFile} representing a complete Dwc-A
 * and produces a {@link DataWorkResult} by calling the given {@link RecordCollectionEvaluator} instance.
 */
public class DataFileRowTypeActor extends AbstractLoggingActor {

  /**
   *
   * @param rowType represent the context under which this actor operates within the {@link DataFile} that will be received
   * @param evaluator
   * @param collector
   */
  public DataFileRowTypeActor(Term rowType, RecordCollectionEvaluator<DataFile> evaluator,
                              CollectorGroupProvider collector) {
    receive(
            //this should only be called once
            match(DataFile.class, dataFileMessage -> {
              pipe(
                      future(() -> processDataFile(dataFileMessage, rowType, evaluator, collector), getContext().dispatcher()),
                      getContext().dispatcher()
              ).to(sender());
            }).build()
    );
  }

  /**
   * Runs the validation and converts the result into a DataWorkResult.
   */
  private DataWorkResult processDataFile(DataFile dwcaDataFile, Term rowType,
                                         RecordCollectionEvaluator<DataFile> evaluator,
                                         CollectorGroupProvider collectorGroupProvider) {
    CollectorGroup collector = collectorGroupProvider.newCollectorGroup();
    try {

      Optional<Stream<RecordEvaluationResult>> evaluatorResult = evaluator.evaluate(dwcaDataFile);

      //this will pull all the results and let the collector decide how to collect it
      evaluatorResult.ifPresent(stream -> stream.forEach(result -> collector.collectResult(result)));

      return new DataWorkResult(rowType, DataWorkResult.Result.SUCCESS, collector);
    } catch (Exception ex) {
      log().error("Error checking records integrity, datafile {}", dwcaDataFile, ex);
      return new DataWorkResult(rowType, DataWorkResult.Result.FAILED, collector);
    }
  }
}
