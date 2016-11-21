package org.gbif.validation.jobserver;


import org.gbif.validation.api.result.ValidationResult;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;

import static akka.japi.pf.ReceiveBuilder.match;

public class JobMonitor extends AbstractLoggingActor {

  /**
   * Creates an MockActor instance that will wait 'waitBeforeDie' before die.
   */
  public  JobMonitor(ActorPropsMapping propsMapping, JobStorage<ValidationResult> jobStorage) {
    receive(
      match(DataJob.class, dataJob -> {
        ActorRef dataMaster = getContext().actorOf(propsMapping.getActorProps(dataJob.getJobData()),
                                                   String.valueOf(dataJob.getJobId()));
        dataMaster.tell(dataJob,self());
      }).
      match(DataJobResult.class, dataJobResult -> {
        jobStorage.put(dataJobResult.getDataJob().getJobId(), (ValidationResult)dataJobResult.getResult().get());
      })
        .matchAny(this::unhandled)
        .build()
    );
  }
}
