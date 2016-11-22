package org.gbif.validation.jobserver;

import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;

import akka.actor.AbstractLoggingActor;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Actor that doesn't perform any action, it waits for a period of time before it kill itself.
 */
public class MockActor extends AbstractLoggingActor {

  /**
   * Creates an MockActor instance that will wait 'waitBeforeDie' before die.
   */
  public  MockActor(long waitBeforeDie) {
    receive(
      match(DataJob.class, dataJob -> {
        try {
          Thread.currentThread().wait(waitBeforeDie);
        } catch (IllegalMonitorStateException ex) {

        }
        JobStatusResponse<ValidationResult>
          result = new JobStatusResponse<ValidationResult>(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(),
                                                           ValidationResultBuilders.Builder
                                                                               .of(true, "mockFile",
                                                                                   FileFormat.TABULAR,
                                                                                   ValidationProfile.GBIF_INDEXING_PROFILE)
                                                                               .build());
        sender().tell(result, self());
      })
        .matchAny(this::unhandled)
        .build()
    );
  }
}
