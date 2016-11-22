package org.gbif.validation.jobserver;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.util.Try;

/**
 * Manages the job submission and statis retrieval.
 */
public class JobServer<T> {



  //Path used to find actors in the system of actors
  private static final String ACTOR_SELECTION_PATH = "/user/JobMonitor/";

  //Default timeout in seconds to get an actor ref
  private static final int ACTOR_SELECTION_TO = 5;

  private static final Logger LOG = LoggerFactory.getLogger(JobServer.class);

  private final ActorSystem system;

  private final AtomicLong jobIdSeed;

  private final JobStorage jobStorage;

  private final ActorRef jobMonitor;

  /**
   * Creates a JobServer instance that will use the jobStore instance to store and retrieve job's data.
   */
  public JobServer(JobStorage jobStorage, ActorPropsMapping<?> propsMapping) {
    system = ActorSystem.create("JobServerSystem");
    jobIdSeed = new AtomicLong(new Date().getTime());
    this.jobStorage = jobStorage;
    jobMonitor = system.actorOf(Props.create(JobMonitor.class,propsMapping,jobStorage),"JobMonitor");
    LOG.info("New jobServer instance created");
  }

  /**
   * Process the submission of a data validation job.
   * If the job is accepted the response contains the new jobId ACCEPTED as the job status.
   */
  public JobStatusResponse<?> submit(DataFile dataFile) {
    long  newJobId = jobIdSeed.getAndIncrement();
    jobMonitor.tell(new DataJob<DataFile>(newJobId, dataFile), jobMonitor);
    return new JobStatusResponse(JobStatusResponse.JobStatus.ACCEPTED, newJobId);
  }

  /**
   * Gets the status of a job. If the job is not found ValidationJobResponse.NOT_FOUND_RESPONSE is returned.
   */
  public JobStatusResponse<?> status(long jobId) {
    //the job storage is checked first
    Optional<JobStatusResponse<?>> result = jobStorage.get(jobId);
    if (result.isPresent()) {
      return result.get();
    }
    //if the job data is not in the storage it might be still running
    return getJobStatus(jobId);
  }

  /**
   * Tries to kill a jobId.
   */
  public JobStatusResponse<?> kill(long jobId) {
    Option<Try<ActorRef>> actorRef = getRunningActor(jobId);
    if (!actorRef.isEmpty()) {
      system.stop(actorRef.get().get());
      return new JobStatusResponse(JobStatus.KILLED, jobId);
    }
    return new JobStatusResponse(JobStatus.NOT_FOUND, jobId);
  }

  /**
   * Stops the jobs server and all the underlying actors.
   */
  public void stop() {
    if (!system.isTerminated()) {
      system.shutdown();
    }
  }

  /**
   * Tries to gets the status from the running instances.
   */
  private JobStatusResponse<?> getJobStatus(long jobId) {
    try {
      //there's a running actor with that jobId name?
      return new JobStatusResponse(getRunningActor(jobId).isEmpty() ? JobStatus.RUNNING : JobStatus.NOT_FOUND,
                                   jobId);
    } catch (Exception ex) {
      LOG.error("Error  retrieving JobId {} data", jobId, ex);
    }
    return JobStatusResponse.NOT_FOUND_RESPONSE;
  }

  /**
   * Tries to get the reference of a running Actor in the system.
   */
  private Option<Try<ActorRef>> getRunningActor(long jobId) {
    ActorSelection sel = system.actorSelection(ACTOR_SELECTION_PATH + jobId);
    Timeout to = new Timeout(ACTOR_SELECTION_TO, TimeUnit.SECONDS);
    return sel.resolveOne(to).value();
  }

}
