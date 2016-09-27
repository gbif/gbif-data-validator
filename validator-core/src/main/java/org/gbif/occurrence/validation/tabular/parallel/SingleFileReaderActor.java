package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;
import static org.gbif.occurrence.validation.util.TempTermsUtils.buildTermMapping;

import java.io.File;
import java.text.MessageFormat;
import java.util.Map;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import static akka.dispatch.Futures.future;
import static akka.pattern.Patterns.pipe;

import static akka.japi.pf.ReceiveBuilder.match;

public class SingleFileReaderActor extends AbstractLoggingActor {

  public SingleFileReaderActor(RecordProcessor recordProcessor) {
    receive(
      match(DataFile.class, dataFile -> {
        pipe(
          future( () -> processDataFile(dataFile, recordProcessor, sender()), getContext().dispatcher()),
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
  private DataWorkResult processDataFile(DataFile dataFile, RecordProcessor recordProcessor, ActorRef sender) {
    try (RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()),
                                                                       dataFile.getDelimiterChar(),
                                                                       dataFile.isHasHeaders(),
                                                                       buildTermMapping(dataFile.getColumns()))) {
      Map<Term, String> record;
      while ((record = recordSource.read()) != null) {
        sender.tell(recordProcessor.process(record), self());
      }

      //add reader aggregated result to the DataWorkResult
      return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS);
    } catch (Exception ex) {
      return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED);
    }
  }

  /**
   * WORK-IN-PROGRESS
   *
   * @param lineNumber
   * @param expectedColumnCount
   * @param actualColumnCount
   * @return
   */
  private static RecordStructureEvaluationResult toColumnCountMismatchEvaluationResult(int lineNumber, int expectedColumnCount,
                                                                                       int actualColumnCount) {
    return new RecordStructureEvaluationResult(Integer.toString(lineNumber),
                                               MessageFormat.format("Column count mismatch: expected {0} columns, got {1} columns",
                                                                    expectedColumnCount, actualColumnCount));
  }


}
