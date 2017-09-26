package org.gbif.validation.jobserver;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.jobserver.impl.InMemoryJobStorage;

import java.nio.file.Paths;
import java.util.UUID;

import akka.actor.Props;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for JobServer class.
 */
public class JobServerTest {

  private JobServer<?> jobServer;

  private JobStorage jobStorage;

  @Before
  public void init() {
    jobStorage = new InMemoryJobStorage();
  }

  @After
  public void stopJobServer() {
    if (jobServer != null) {
      jobServer.stop();
    }
  }

  private static void silentCallback(UUID key){}


  /**
   * Tests that a Job can be submitted.
   */
  @Test
  public void submitTestIT() {
    jobServer = new JobServer<>(jobStorage, () -> Props.create(MockActor.class, 0L), JobServerTest::silentCallback);
    JobStatusResponse<?> jobResponse = jobServer.submit(createNewDataFile());
    Assert.assertEquals(JobStatusResponse.JobStatus.ACCEPTED, jobResponse.getStatus());
    Assert.assertNotEquals(0L, jobResponse.getJobId());
  }

  /**
   * Tests that a Job can be submitted.
   */
  @Test
  public void submitAndGetTestIT() {
    jobServer = new JobServer<>(jobStorage, () -> Props.create(MockActor.class, 2000L), JobServerTest::silentCallback);
    JobStatusResponse<?> initialJobResponse = jobServer.submit(createNewDataFile());
    JobStatusResponse<?> jobResponse = jobServer.status(initialJobResponse.getJobId());
    Assert.assertEquals(JobStatusResponse.JobStatus.RUNNING, jobResponse.getStatus());
    Assert.assertEquals(jobResponse.getJobId(), initialJobResponse.getJobId());
  }


  @Test
  public void notFoundTestIT() {
    jobServer = new JobServer<>(jobStorage, () -> Props.create(MockActor.class, 0L), JobServerTest::silentCallback);
    JobStatusResponse<?> jobResponse = jobServer.status(100l);
    Assert.assertEquals(JobStatusResponse.JobStatus.NOT_FOUND, jobResponse.getStatus());
  }

  @Test
  public void killTestIT() throws InterruptedException {
    jobServer = new JobServer<>(jobStorage, () -> Props.create(MockActor.class, 2000L), JobServerTest::silentCallback);
    JobStatusResponse<?> initialJobResponse = jobServer.submit(createNewDataFile());
    Thread.sleep(5); //sleep before getting the status of a running actor
    JobStatusResponse<?> jobResponse = jobServer.status(initialJobResponse.getJobId());
    Assert.assertEquals(JobStatusResponse.JobStatus.RUNNING, jobResponse.getStatus());
    JobStatusResponse<?> jobKillResponse = jobServer.kill(initialJobResponse.getJobId());
    Assert.assertEquals(JobStatusResponse.JobStatus.KILLED, jobKillResponse.getStatus());
    JobStatusResponse<?> jobSecondKillResponse = jobServer.status(jobKillResponse.getJobId());
    Assert.assertEquals(JobStatusResponse.JobStatus.KILLED, jobSecondKillResponse.getStatus());
  }

  private static DataFile createNewDataFile(){
    return new DataFile(UUID.randomUUID(), Paths.get(""), "", FileFormat.TABULAR, "", "");
  }

}
