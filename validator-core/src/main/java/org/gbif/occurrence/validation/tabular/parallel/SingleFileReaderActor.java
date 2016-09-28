package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.model.StructureEvaluationDetailType;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.Callable;

import akka.actor.UntypedActor;

import static org.gbif.occurrence.validation.util.TempTermsUtils.buildTermMapping;

import static akka.dispatch.Futures.future;
import static akka.pattern.Patterns.pipe;

public class SingleFileReaderActor extends UntypedActor {

  private final RecordProcessor recordProcessor;

  public SingleFileReaderActor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataFile) {
      doWork((DataFile) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(DataFile dataFile) throws IOException {
    pipe(future(new Callable<DataWorkResult>() {
      @Override
      public DataWorkResult call() throws Exception {
        return processDataFile(dataFile);
      }
    }, getContext().dispatcher()),getContext().dispatcher()).to(getSender());
  }

  private DataWorkResult processDataFile(DataFile dataFile) throws IOException {
    try( RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()),
                                                                       dataFile.getDelimiterChar(),
                                                                       dataFile.isHasHeaders(),
                                                                       buildTermMapping(dataFile.getColumns()))) {
      long line = dataFile.isHasHeaders() ? 1 : 0;
      line += dataFile.getFileLineOffset();

      int expectedNumberOfColumn = dataFile.getColumns().length;
      Map<Term, String> record;

      while ((record = recordSource.read()) != null) {
        line++;
        if(record.size() != expectedNumberOfColumn){
          getSender().tell(toColumnCountMismatchEvaluationResult(line, expectedNumberOfColumn, record.size()), getSelf());
        }
        getSender().tell(recordProcessor.process(Long.toString(line), record), getSelf());
      }

      //add reader aggregated result to the DataWorkResult
      return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS);
    } catch (Exception ex) {
      return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED);
    }
  }

  /**
   * Creates a RecordStructureEvaluationResult instance for a column count mismatch.
   *
   * @param lineNumber
   * @param expectedColumnCount
   * @param actualColumnCount
   * @return
   */
  private static RecordStructureEvaluationResult toColumnCountMismatchEvaluationResult(long lineNumber, int expectedColumnCount,
                                                                                       int actualColumnCount) {
    //FIXME record line number
    return new RecordStructureEvaluationResult.Builder().addDetail(StructureEvaluationDetailType.RECORD_STRUCTURE,
            MessageFormat.format("Column count mismatch: expected {0} columns, got {1} columns",
                    expectedColumnCount, actualColumnCount)).build();
  }

}
