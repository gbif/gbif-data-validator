package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.result.ValidationIssues;
import org.gbif.validation.api.result.ValidationResultElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * WIP, C.G.
 */
public class ParallelDataFileProcessorMasterTest {

  private static final String TEST_FILE_LOCATION = "validator_test_file_all_issues.tsv";
  private static final String UAT_API = "http://api.gbif-uat.org/v1";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

 // private final ActorSystem system = ActorSystem.create("ParallelDataFileProcessorMasterTestSystem");

//  @Test
//  public void test(){

//    EvaluatorFactory EVALUATOR_FACTORY = new EvaluatorFactory(UAT_API);

//    Props props =  Props.create(DataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
//            workingDir, checklistValidator);
//
//    //creates a actor that is responsible to handle a this jobData
//    ActorRef jobMaster = system.actorOf(propsSupplier.get(),
//            String.valueOf(dataJob.getJobId())); //the jobId used as Actor's name
//    jobMaster.tell(dataJob, self());
 // }

  @Test
  public void testMergeIssuesOnFilename() {
    List<ValidationResultElement> source = new ArrayList<>();
    List<ValidationResultElement> mergeInto = new ArrayList<>();

    source.add(ValidationResultElement.forMetadata("test.txt", Collections.singletonList(
            ValidationIssues.withEvaluationTypeOnly(EvaluationType.LICENSE_MISSING_OR_UNKNOWN))));

    mergeInto.add(new ValidationResultElement("test.txt", 18L, DwcFileType.CORE, DwcTerm.Occurrence,
            Lists.newArrayList(ValidationIssues.withSample(EvaluationType.INDIVIDUAL_COUNT_INVALID, 1,
                    Collections.emptyList()))));

    mergeInto.add(new ValidationResultElement("test2.txt", 18L, DwcFileType.CORE, DwcTerm.Occurrence,
            Lists.newArrayList(ValidationIssues.withSample(EvaluationType.INDIVIDUAL_COUNT_INVALID, 1,
                    Collections.emptyList()))));

    DataFileProcessorMaster.mergeIssuesOnFilename(source, mergeInto);

    assertEquals(2, mergeInto.size());
    ValidationResultElement testTxtElement = mergeInto.get(0);
    assertEquals("test.txt", testTxtElement.getFileName());
    assertEquals(2, testTxtElement.getIssues().size());
    //assert that the LICENSE_MISSING_OR_UNKNOWN is now attached to "test.txt" in the mergeInto collection
    assertTrue(testTxtElement.getIssues().stream()
            .filter( issue -> EvaluationType.LICENSE_MISSING_OR_UNKNOWN == issue.getIssue())
            .findFirst()
            .isPresent());
  }

}
