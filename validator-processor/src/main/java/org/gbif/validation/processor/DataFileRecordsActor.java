package org.gbif.validation.processor;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
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
 * Akka actor that processes a single {@link DataFile} at the record level.
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
            match(DataFile.class, dataFile -> {
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
  private static DataWorkResult processDataFile(DataFile dataFile, RecordEvaluator recordEvaluator, CollectorGroup collectors) {
    long line = dataFile.getFileLineOffset().orElse(0) + 1; //we report line number starting at 1
    try (RecordSource recordSource = RecordSourceFactory.fromDataFile(dataFile).orElse(null)) {
      //Term rowType = dataFile.getRowType();
      String[] record;
      while ((record = recordSource.read()) != null) {
        line++;
        collectors.collectMetrics(record);
        collectors.collectResult(recordEvaluator.evaluate(line, record));
      }
      return new DataWorkResult(dataFile.getRowType(), DataWorkResult.Result.SUCCESS, collectors);
    } catch (Exception ex) {
      LOG.error("Error while evaluating line {} of {}", line, dataFile.getFilePath(), ex);
      return new DataWorkResult(dataFile.getRowType(), DataWorkResult.Result.FAILED, collectors);
    }
  }

}
