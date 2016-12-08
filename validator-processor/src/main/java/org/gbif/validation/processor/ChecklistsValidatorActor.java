package org.gbif.validation.processor;

import org.gbif.validation.api.result.ChecklistValidationResult;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.checklists.ChecklistValidator;

import akka.actor.AbstractLoggingActor;

import static akka.dispatch.Futures.future;
import static akka.japi.pf.ReceiveBuilder.match;
import static akka.pattern.Patterns.pipe;

/**
 * Akka actor that wraps the Checklists validation.
 */
public class ChecklistsValidatorActor extends AbstractLoggingActor {

  private final ChecklistValidator validator;

   public ChecklistsValidatorActor(ChecklistValidator validator) {
     this.validator = validator;
     receive(
       //this should only be called once
       match(DataFile.class, dataFileMessage -> {
         pipe(
           future(() -> processDataFile(dataFileMessage), getContext().dispatcher()),
           getContext().dispatcher()
         ).to(sender());
       }).build()
     );
   }

  /**
   * Runs the validation and converts the result into a DataWorkResult.
   */
  private DataWorkResult processDataFile(DataFile dataFile) {
    DataWorkResult dataWorkResult = new DataWorkResult();
    dataWorkResult.setDataFile(dataFile);
    try {
      ChecklistValidationResult result = validator.validate(dataFile);
      dataWorkResult.setChecklistValidationResult(result);
      dataWorkResult.setResult(DataWorkResult.Result.SUCCESS);
    } catch (Exception ex) {
      log().error("Error validation checklist, datafile {}", dataFile, ex);
      dataWorkResult.setResult(DataWorkResult.Result.FAILED);
    }
    return dataWorkResult;
  }

}
