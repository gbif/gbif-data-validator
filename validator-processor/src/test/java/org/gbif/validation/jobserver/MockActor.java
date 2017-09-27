package org.gbif.validation.jobserver;

import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.jobserver.messages.DataJob;

import java.util.UUID;

import akka.actor.AbstractLoggingActor;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Actor that doesn't perform any action, it waits for a period of time before it kill itself.
 */
public class MockActor extends AbstractLoggingActor {

  /**
   * Creates an MockActor instance that will wait 'waitBeforeDie' before die.
   */
  public MockActor(long waitBeforeDie) {
    receive(
      match(DataJob.class, dataJob -> {
        Thread.sleep(waitBeforeDie);
        JobStatusResponse<ValidationResult>
          result = new JobStatusResponse<>(JobStatusResponse.JobStatus.FINISHED, dataJob.getJobId(),
                dataJob.getStartTimeStamp(), UUID.randomUUID(),
                new ValidationResult(true, "mockFile", FileFormat.TABULAR, "", ValidationProfile.GBIF_INDEXING_PROFILE,
                        null));
        sender().tell(result, self());
      }).matchAny(this::unhandled)
        .build()
    );
  }
}
