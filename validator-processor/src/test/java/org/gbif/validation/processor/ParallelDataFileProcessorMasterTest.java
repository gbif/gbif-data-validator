package org.gbif.validation.processor;

import org.gbif.validation.TestUtils;
import org.gbif.validation.conf.ValidatorConfiguration;
import org.gbif.validation.jobserver.JobMonitor;
import org.gbif.validation.jobserver.JobStorage;
import org.gbif.validation.jobserver.impl.ActorPropsSupplier;
import org.gbif.validation.jobserver.impl.InMemoryJobStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * WIP, C.G.
 */
public class ParallelDataFileProcessorMasterTest {

  private static final String TEST_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final String UAT_API = "http://api.gbif-uat.org/v1";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private final ActorSystem system = ActorSystem.create("ParallelDataFileProcessorMasterTestSystem");

  @Test
  public void test() throws IOException {

   // EvaluatorFactory EVALUATOR_FACTORY = new EvaluatorFactory(UAT_API);

    Path workingFolder = folder.newFolder().toPath();
    JobStorage js = new InMemoryJobStorage();

    ValidatorConfiguration validatorConfiguration = TestUtils.getValidatorConfiguration();
    ActorPropsSupplier aps =  new ActorPropsSupplier(TestUtils.getEvaluatorFactory(),
            5000, workingFolder.toString(), false);

    Consumer<Long> completionCallback = ParallelDataFileProcessorMasterTest::onCompletion;
    ActorRef jobMonitor = system.actorOf(Props.create(JobMonitor.class, aps, js, completionCallback), "JobMonitor");


//    DataFile df = DataFileFactory.newDataFile(Paths.get("dwcarchive-1"),
//            "dwcarchive-1.zip", FileFormat.DWCA , "application/zip", "application/zip");
//
//    //creates a actor that is responsible to handle a this jobData
//    //jobMonitor.tell(new DataJob<>(1, df), jobMonitor);
//
//    while(!js.getStatus(1).isPresent() || js.getStatus(1).get().getStatus() != JobStatusResponse.JobStatus.FINISHED) {
//      try {
//        Thread.sleep(15);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//    }
  }
  
  private static void onCompletion(Long jobId) {

  }

}
