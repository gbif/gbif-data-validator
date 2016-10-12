package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;

import java.io.File;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Akka actor that processes a single occurrence data file.
 */
public class SingleFileReaderActor extends AbstractLoggingActor {

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
    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()),
                                                                       dataFile.getDelimiterChar(),
                                                                       dataFile.isHasHeaders())) {
      long line = dataFile.getFileLineOffset();

      int expectedNumberOfColumn = dataFile.getColumns().length;
      String[] record;

      while ((record = recordSource.read()) != null) {
        line++;
        if (record.length != expectedNumberOfColumn) {
          //TODO get a list of recordEvaluators
          //sender.tell(toColumnCountMismatchEvaluationResult(line, expectedNumberOfColumn, record.size()), self());
        }
        sender.tell(recordEvaluator.evaluate(line, record), self());
      }

      //add reader aggregated result to the DataWorkResult
      return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS);
    } catch (Exception ex) {
      return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED);
    }
  }

}
