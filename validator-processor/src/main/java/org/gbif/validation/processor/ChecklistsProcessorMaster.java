package org.gbif.validation.processor;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import static org.gbif.validation.ChecklistValidator.validate;
import org.gbif.validation.api.ChecklistValidationResult;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.jobserver.messages.DataJob;


import akka.actor.AbstractLoggingActor;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Akka actor that wraps the Checklists validation.
 */
public class ChecklistsProcessorMaster extends AbstractLoggingActor {

   public ChecklistsProcessorMaster(NormalizerConfiguration configuration) {
     receive(
       //this should only be called once
       match(DataJob.class, dataJobMessage -> {
         try {
           ChecklistValidationResult result = validate((DataFile) dataJobMessage.getJobData(), configuration);
           context().parent().tell(new JobStatusResponse<>(JobStatus.FINISHED, dataJobMessage.getJobId(), result), self());
         } catch (Exception ex) {
           log().error("Error validation checklist, jobId {}", dataJobMessage.getJobId(), ex);
           context().parent().tell(new JobStatusResponse<>(JobStatus.FAILED, dataJobMessage.getJobId()), self());
         }
       }).build()
     );
   }

}
