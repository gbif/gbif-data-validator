package org.gbif.validation.processor;

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

 // private final ActorSystem system = ActorSystem.create("ParallelDataFileProcessorMasterTestSystem");

  @Test
  public void test(){

//    EvaluatorFactory EVALUATOR_FACTORY = new EvaluatorFactory(UAT_API);

//    Props props =  Props.create(DataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
//            workingDir, checklistValidator);
//
//    //creates a actor that is responsible to handle a this jobData
//    ActorRef jobMaster = system.actorOf(propsSupplier.get(),
//            String.valueOf(dataJob.getJobId())); //the jobId used as Actor's name
//    jobMaster.tell(dataJob, self());
  }


}
