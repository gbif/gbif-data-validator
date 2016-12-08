package org.gbif.validation.jobserver;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.JobStatusResponse.JobStatus;
import org.gbif.validation.jobserver.messages.DataJob;

import static  org.gbif.validation.jobserver.util.ActorSelectionUtil.getRunningActor;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the job submission and status retrieval.
 */
public class JobServer<T> {

  private static final Logger LOG = LoggerFactory.getLogger(JobServer.class);

  private final ActorSystem system;

  private final AtomicLong jobIdSeed;

  private final JobStorage jobStorage;

  private final ActorRef jobMonitor;

  /**
   * Creates a JobServer instance that will use the jobStore instance to store and retrieve job's data.
   */
  public JobServer(JobStorage jobStorage, Supplier<Props> propsSupplier) {
    system = ActorSystem.create("JobServerSystem");
    jobIdSeed = new AtomicLong(new Date().getTime());
    this.jobStorage = jobStorage;
    jobMonitor = system.actorOf(Props.create(JobMonitor.class, propsSupplier, jobStorage), "JobMonitor");
    LOG.info("New jobServer instance created");
  }

  /**
   * Process the submission of a data validation job.
   * If the job is accepted the response contains the new jobId ACCEPTED as the job status.
   */
  public JobStatusResponse<?> submit(DataFile dataFile) {
    long  newJobId = jobIdSeed.getAndIncrement();
    jobMonitor.tell(new DataJob<>(newJobId, dataFile), jobMonitor);
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
    Optional<ActorRef> actorOpt = getRunningActor(jobId, system);
    if (actorOpt.isPresent()) {
      JobStatusResponse<?> response = new JobStatusResponse(JobStatus.KILLED, jobId);
      ActorRef actorRef = actorOpt.get();
      actorRef.tell(Kill.getInstance(), jobMonitor);
      system.stop(actorRef);
      jobStorage.put(response);   //stores a job result
      return response;
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
      return new JobStatusResponse(getRunningActor(jobId, system).isPresent() ? JobStatus.RUNNING : JobStatus.NOT_FOUND,
                                   jobId);
    } catch (Exception ex) {
      LOG.error("Error  retrieving JobId {} data", jobId, ex);
    }
    return JobStatusResponse.NOT_FOUND_RESPONSE;
  }

}
