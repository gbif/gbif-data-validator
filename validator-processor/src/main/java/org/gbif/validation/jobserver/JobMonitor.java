package org.gbif.validation.jobserver;


import org.gbif.validation.api.model.JobStatusResponse;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;

import static akka.japi.pf.ReceiveBuilder.match;

/**
 * Actor that acts as the main controller to coordinate the creation of jobs and store results.
 */
public class JobMonitor extends AbstractLoggingActor {

  /**
   * Creates an MockActor instance that will wait 'waitBeforeDie' before die.
   */
  public  JobMonitor(ActorPropsMapping propsMapping, JobStorage jobStorage) {
    receive(
      match(DataJob.class, dataJob -> {
        //creates a actor that is responsible to handle a this jobData
        ActorRef jobMaster = getContext().actorOf(propsMapping.getActorProps(dataJob.getJobData()),
                                                   String.valueOf(dataJob.getJobId())); //the jobId used as Actor's name
        jobMaster.tell(dataJob, self());
      }).
      match(JobStatusResponse.class, dataJobResult -> {
        jobStorage.put(dataJobResult);   //stores a job result
      })
        .matchAny(this::unhandled)
        .build()
    );
  }
}
