package org.gbif.validation.processor;

import org.gbif.validation.evaluator.EvaluatorFactory;

import akka.actor.ActorSystem;
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
  public void test(){

    EvaluatorFactory EVALUATOR_FACTORY = new EvaluatorFactory(UAT_API);

//    Props props =  Props.create(DataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
//            workingDir, checklistValidator);
//
//    //creates a actor that is responsible to handle a this jobData
//    ActorRef jobMaster = system.actorOf(propsSupplier.get(),
//            String.valueOf(dataJob.getJobId())); //the jobId used as Actor's name
//    jobMaster.tell(dataJob, self());
  }

//  public void testSingleFileValidation() throws IOException {
//
//    File testFile = FileUtils.getClasspathFile(TEST_FILE_LOCATION);
//
//    ResourceEvaluationManager manager = new ResourceEvaluationManager(DEV_API, 1000);
//    DataFile datafile = new DataFile();
//    datafile.setHasHeaders(Optional.of(true));
//    datafile.setDelimiterChar('\t');
//    datafile.setSourceFileName("myfile.tsv");
//    datafile.setRowType(DwcTerm.Occurrence);
//    datafile.setFileFormat(FileFormat.TABULAR);
//    datafile.setFilePath(testFile.toPath());
//
//    ValidationResult result = manager.evaluate(datafile);
//
//    assertNotNull(result.getResults());
//    // we should have 1 ValidationResourceResult which should matches NUMBER_OF_ISSUES_EXPECTED
//    assertEquals(NUMBER_OF_ISSUES_EXPECTED, result.getResults().get(0).getIssues().size());
//  }

}
