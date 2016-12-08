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
 * Akka actor that processes a single {@link DataFile}.
 */
class SingleFileReaderActor extends AbstractLoggingActor {

  private static final Logger LOG = LoggerFactory.getLogger(SingleFileReaderActor.class);

  SingleFileReaderActor(RecordEvaluator recordEvaluator, CollectorGroupProvider collectorsProvider) {
    receive(
            match(DataFile.class, dataFile -> {
              pipe(
                      future(() -> processDataFile(dataFile, recordEvaluator, collectorsProvider.newCollectorGroup()), getContext().dispatcher()),
                      getContext().dispatcher()
              ).to(sender());
            })
                    .matchAny(this::unhandled)
                    .build()
    );
  }

  /**
   * Process a datafile using a record processor.
   * The sender is sent as parameter because the real sender is only known in the context of receiving messages.
   */
  private static DataWorkResult processDataFile(DataFile dataFile, RecordEvaluator recordEvaluator, CollectorGroup collectors) {
    long line = dataFile.getFileLineOffset().orElse(0);
    try (RecordSource recordSource = RecordSourceFactory.fromDataFile(dataFile).orElse(null)) {
      //Term rowType = dataFile.getRowType();
      String[] record;
      while ((record = recordSource.read()) != null) {
        line++;
        collectors.getMetricsCollector().collect(record);
        long finalLine = line;
        String[] finalRecord = record; //required to be used in lambda expressions
        collectors.getRecordsCollectors().forEach(c -> c.collect(recordEvaluator.evaluate(finalLine, finalRecord)));
      }
      return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS, collectors);
    } catch (Exception ex) {
      LOG.error("Error while evaluating line {} of {}", line, dataFile.getFilePath(), ex);
      return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED, collectors);
    }
  }

}
