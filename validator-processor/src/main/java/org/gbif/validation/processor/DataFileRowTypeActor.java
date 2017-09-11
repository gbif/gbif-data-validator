package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;

import java.util.Objects;

import akka.actor.AbstractLoggingActor;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Simple actor that receives a {@link DataFile} representing a complete Dwc-A
 * and produces a {@link DataWorkResult} by calling the given {@link RecordCollectionEvaluator} instance.
 * {@link DataFileRowTypeActor} works on a specific rowType but expect the dataFile to represent the complete
 * Dwc-A.
 */
class DataFileRowTypeActor extends AbstractLoggingActor {

  /**
   *
   * @param rowTypeKey represent the context under which this actor operates within the {@link DataFile} that will be received
   * @param evaluator
   * @param collector
   */
  public DataFileRowTypeActor(RowTypeKey rowTypeKey, RecordCollectionEvaluator evaluator,
                              CollectorGroupProvider collector) {
    Objects.requireNonNull(rowTypeKey, "rowTypeKey shall be provided");
    receive(
            //this should only be called once
            match(DwcDataFile.class, dataFileMessage -> {
              pipe(
                      future(() -> processDataFile(dataFileMessage, rowTypeKey, evaluator, collector), getContext().dispatcher()),
                      getContext().dispatcher()
              ).to(sender());
            }).build()
    );
  }

  /**
   * Runs the validation and converts the result into a DataWorkResult.
   */
  private DataWorkResult processDataFile(DwcDataFile dwcaDataFile, RowTypeKey rowTypeKey,
                                         RecordCollectionEvaluator evaluator,
                                         CollectorGroupProvider collectorGroupProvider) {
    CollectorGroup collector = collectorGroupProvider.newCollectorGroup();
    try {

      evaluator.evaluate(dwcaDataFile, collector::collectResult);

      return new DataWorkResult(rowTypeKey, dwcaDataFile.getDataFile().getSourceFileName(), DataWorkResult.Result.SUCCESS, collector);
    } catch (Exception ex) {
      log().error(ex, "Error checking records integrity, datafile {}", dwcaDataFile);
      return new DataWorkResult(rowTypeKey, dwcaDataFile.getDataFile().getSourceFileName(), DataWorkResult.Result.FAILED, collector);
    }
  }
}
