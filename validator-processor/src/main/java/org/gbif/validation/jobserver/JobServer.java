package org.gbif.validation.jobserver;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.ValidationJobResponse;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.model.ValidationJobResponse.JobStatus;

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

/**
 * Manages the job submission and statis retrieval.
 */
public class JobServer {

  //Prefix used to name actor
  private static final String JOB_NAME_PREFIX = "DataValidation_";

  //Path used to find actors in the system of actors
  private static final String ACTOR_SELECTION_PATH = "/user/" + JOB_NAME_PREFIX;

  //Default timeout in seconds to get an actor ref
  private static final int ACTOR_SELECTION_TO = 5;

  private static final Logger LOG = LoggerFactory.getLogger(JobServer.class);

  private final ActorSystem system;

  private final AtomicLong jobIdSeed;

  private final JobStorage<ValidationResult> jobStorage;

  /**
   * Creates a JobServer instance that will use the jobStore instance to store and retrieve job's data.
   */
  public JobServer(JobStorage<ValidationResult> jobStorage) {
    system = ActorSystem.create("JobServerSystem");
    jobIdSeed = new AtomicLong(new Date().getTime());
    this.jobStorage = jobStorage;
    LOG.info("New jobServer instance created");
  }

  /**
   * Process the submission of a data validation job.
   * If the job is accepted the response contains the new jobId ACCEPTED as the job status.
   */
  public ValidationJobResponse submit(Props actorConf, DataFile dataFile) {
    long  newJobId = jobIdSeed.getAndIncrement();
    ActorRef master = system.actorOf(actorConf, JOB_NAME_PREFIX + newJobId);
    master.tell(dataFile, master);
    return new ValidationJobResponse(ValidationJobResponse.JobStatus.ACCEPTED, newJobId);
  }

  /**
   * Gets the status of a job. If the job is not found ValidationJobResponse.NOT_FOUND is returned.
   */
  public ValidationJobResponse status(long jobId) {
    //the job storage is checked first
    Optional<ValidationResult> result = jobStorage.get(jobId);
    if (result.isPresent()) {
      return new ValidationJobResponse(ValidationJobResponse.JobStatus.FINISHED, jobId, result.get());
    }
    //if the job data is not in the storage it might be still running
    return getJobStatus(jobId);
  }

  /**
   * Tries to gets the status from the running instances.
   */
  private ValidationJobResponse getJobStatus(long jobId) {
    try {
      ActorSelection sel = system.actorSelection(ACTOR_SELECTION_PATH + jobId);
      Timeout to = new Timeout(ACTOR_SELECTION_TO, TimeUnit.SECONDS);
      //there's a running actor with that jobId name?
      return new ValidationJobResponse(sel.resolveOne(to).value().isEmpty() ? JobStatus.RUNNING : JobStatus.NOT_FOUND,
                                       jobId);
    } catch (Exception ex) {
      LOG.error("Error  retrieving JobId {} data", jobId, ex);
    }
    return ValidationJobResponse.NOT_FOUND;
  }

}
