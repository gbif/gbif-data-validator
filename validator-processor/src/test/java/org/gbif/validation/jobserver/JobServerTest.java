package org.gbif.validation.jobserver;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.jobserver.impl.InMemoryJobStorage;

import akka.actor.Props;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for JobServer class.
 */
public class JobServerTest {

  private static JobServer jobServer;

  private static JobStorage jobStorage;

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


  /**
   * Tests that a Job can be submitted.
   */
  @Test
  public void submitTestIT() {
    jobServer = new JobServer(jobStorage, x -> Props.create(MockActor.class, 0L));
    JobStatusResponse jobResponse = jobServer.submit(new DataFile());
    Assert.assertEquals(JobStatusResponse.JobStatus.ACCEPTED, jobResponse.getStatus());
    Assert.assertNotEquals(0L, jobResponse.getJobId());
  }

  /**
   * Tests that a Job can be submitted.
   */
  @Test
  public void submitAndGetTestIT() {
    jobServer = new JobServer(jobStorage, x -> Props.create(MockActor.class, 2L));
    JobStatusResponse initialJobResponse = jobServer.submit(new DataFile());
    JobStatusResponse jobResponse = jobServer.status(initialJobResponse.getJobId());
    Assert.assertEquals(JobStatusResponse.JobStatus.RUNNING, jobResponse.getStatus());
    Assert.assertEquals(jobResponse.getJobId(), initialJobResponse.getJobId());
  }

}
