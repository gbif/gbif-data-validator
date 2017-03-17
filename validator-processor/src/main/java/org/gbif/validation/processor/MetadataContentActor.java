package org.gbif.validation.processor;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.List;

import akka.actor.AbstractLoggingActor;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Akka actor that processes the metdata document of a {@link DwcDataFile}
 *
 */
class MetadataContentActor extends AbstractLoggingActor {

  public MetadataContentActor(DwcDataFileEvaluator metadataEvaluator) {
    receive(
            //this should only be called once
            match(DwcDataFile.class, dataFileMessage -> {
              pipe(
                      future(() -> processDataFile(dataFileMessage, metadataEvaluator), getContext().dispatcher()),
                      getContext().dispatcher()
              ).to(sender());
            }).build()
    );
  }

  /**
   * Runs the validation and converts the result into a {@link MetadataWorkResult}.
   */
  private MetadataWorkResult processDataFile(DwcDataFile dwcaDataFile, DwcDataFileEvaluator metadataEvaluator) {
    try {
      List<ValidationResultElement> evaluatorResult =
              metadataEvaluator.evaluate(dwcaDataFile).orElse(null);
      return new MetadataWorkResult(DataWorkResult.Result.SUCCESS, evaluatorResult);
    } catch (Exception ex) {
      log().error(ex, "Error running MetadataContentActor, datafile {}", dwcaDataFile);
      return new MetadataWorkResult(DataWorkResult.Result.FAILED, null);
    }
  }

}
