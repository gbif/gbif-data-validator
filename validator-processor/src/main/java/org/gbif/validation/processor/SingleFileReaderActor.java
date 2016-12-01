package org.gbif.validation.processor;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.source.RecordSourceFactory;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Akka actor that processes a single occurrence data file.
 */
public class SingleFileReaderActor extends AbstractLoggingActor {

  private static final Logger LOG = LoggerFactory.getLogger(SingleFileReaderActor.class);

  public SingleFileReaderActor(RecordEvaluator recordEvaluator) {
    receive(
            match(DataFile.class, dataFile -> {
              pipe(
                      future(() -> processDataFile(dataFile, recordEvaluator, sender()), getContext().dispatcher()),
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
  private DataWorkResult processDataFile(DataFile dataFile, RecordEvaluator recordEvaluator, ActorRef sender) {

    try (RecordSource recordSource = RecordSourceFactory.fromDataFile(dataFile).orElse(null)) {
      Term rowType = dataFile.getRowType();
      long line = dataFile.getFileLineOffset().orElse(0);
      String[] record;
      while ((record = recordSource.read()) != null) {
        line++;
        sender.tell(new DataLine(rowType, record), self());
        sender.tell(recordEvaluator.evaluate(line, record), self());
      }
      //add reader aggregated result to the DataWorkResult
      return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS);
    } catch (Exception ex) {
      LOG.error("", ex);
      return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED);
    }
  }

}
