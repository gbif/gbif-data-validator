package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.source.RecordSourceFactory;

import akka.actor.AbstractLoggingActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Akka actor that processes a single {@link DataFile} (representing a fragment or a complete set of records)
 * at the record level.
 *
 */
class DataFileRecordsActor extends AbstractLoggingActor {

  private static final Logger LOG = LoggerFactory.getLogger(DataFileRecordsActor.class);

  /**
   * Creates a new instance of {@link DataFileRecordsActor} that can receive {@link DataFile} messages.
   *
   * @param recordEvaluator    {@link RecordEvaluator} to use on each record of the {@link DataFile}
   * @param collectorsProvider provider of {@link CollectorGroup} to get an new instance for each {@link DataFile}
   *                           messages.
   */
  DataFileRecordsActor(RecordEvaluator recordEvaluator, CollectorGroupProvider collectorsProvider) {
    receive(
            match(TabularDataFile.class, dataFile -> {
              pipe(
                      future(() -> processDataFile(dataFile, recordEvaluator, collectorsProvider.newCollectorGroup()),
                              getContext().dispatcher()),
                      getContext().dispatcher()
              ).to(sender());
            })
                    .matchAny(this::unhandled)
                    .build()
    );
  }

  /**
   * Process a {@link DataFile} by opening a {@link RecordSource} and evaluating all records
   * using a {@link RecordEvaluator}. All records and evaluation results are also sent to the {@link CollectorGroup}.
   *
   * @param dataFile
   * @param recordEvaluator
   * @param collectors
   * @return
   */
  private DataWorkResult processDataFile(TabularDataFile dataFile, RecordEvaluator recordEvaluator, CollectorGroup collectors) {
    //add one if there is a header since the source will not send it
    long lineNumber = dataFile.getFileLineOffset().orElse(0) + (dataFile.isHasHeaders() ? + 1 : 0);
    log().info("Starting to read: " + dataFile.getFilePath());
    try (RecordSource recordSource = RecordSourceFactory.fromTabularDataFile(dataFile)) {
      String[] record;
      while ((record = recordSource.read()) != null) {
        //we report line number starting at 1 so we will increment
        //the counter before reporting the line number
        lineNumber++;
        collectors.collectMetrics(record);
        collectors.collectResult(recordEvaluator.evaluate(lineNumber, record));
      }
      log().info("Done reading: " + dataFile.getFilePath() + " finished at line " + lineNumber + " (including offset)");
      return new DataWorkResult(dataFile.getRowType(), DataWorkResult.Result.SUCCESS, collectors);
    } catch (Exception ex) {
      log().error("Error while evaluating line {} of {}: {}", lineNumber, dataFile.getFilePath(), ex.getMessage());
      return new DataWorkResult(dataFile.getRowType(), DataWorkResult.Result.FAILED, collectors);
    }
  }

}
