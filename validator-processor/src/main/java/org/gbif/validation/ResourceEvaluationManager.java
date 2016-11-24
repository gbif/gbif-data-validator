package org.gbif.validation;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.DataFileProcessorAsync;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.JobStatusResponse;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorFactory;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.single.SingleDataFileProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ResourceEvaluationManager} is responsible to create and trigger evaluations.
 *
 */
public class ResourceEvaluationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceEvaluationManager.class);

  private final EvaluatorFactory factory;
  private final Integer fileSplitSize;

  private final ActorSystem system;

  private final AtomicLong newJobId = new AtomicLong(new Date().getTime());

  /**
   *
   * @param apiUrl
   * @param fileSplitSize threshold (in number of lines) until we use the parallel processing.
   */
  public ResourceEvaluationManager(String apiUrl, Integer fileSplitSize){
    factory = new EvaluatorFactory(apiUrl);
    this.fileSplitSize = fileSplitSize;
    system = ActorSystem.create("DataFileProcessorSystem");
  }

  /**
   * Trigger the entire evaluation process for a {@link DataFile}.
   *
   * @param dataFile
   * @return
   * @throws IOException
   */
  public ValidationResult evaluate(DataFile dataFile) throws IOException {

    //Validate the structure of the resource
    Optional<ValidationResultElement> validationResultElement =
      EvaluatorFactory.createResourceStructureEvaluator(dataFile.getFileFormat())
        .evaluate(dataFile.getFilePath(), dataFile.getSourceFileName());

    if(validationResultElement.isPresent()) {
      return ValidationResultBuilders.Builder.of(false, dataFile.getSourceFileName(),
                                                 dataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE)
        .withResourceResult(validationResultElement.get()).build();
    }

    //prepare the resource
    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);

    int maxNumOfLine = preparedDataFiles.stream().mapToInt(DataFile::getNumOfLines).max().getAsInt();

    if (maxNumOfLine <= fileSplitSize) {
      return runEvaluation(dataFile.getSourceFileName(), dataFile.getFileFormat(), preparedDataFiles);
    }

    return runEvaluationWithSplit(dataFile.getSourceFileName(), dataFile.getFileFormat(), preparedDataFiles);
  }

  /**
   * Split files and run evaluation on all of them.
   *
   * @param sourceFileName
   * @param fileFormat
   * @param dataFiles
   * @return
   */
  private ValidationResult runEvaluationWithSplit(String sourceFileName, FileFormat fileFormat,
                                                  List<DataFile> dataFiles)  {

    ValidationResultBuilders.Builder blrd = ValidationResultBuilders.Builder.of(true, sourceFileName,
                                                                                fileFormat, ValidationProfile.GBIF_INDEXING_PROFILE);
    //FIX ME .get(0) should be a loop when we can create multiples ParallelDataFileProcessor
    blrd.withResourceResult(buildDataFileProcessorWithSplit(dataFiles.get(0)).process(dataFiles.get(0)));
    return blrd.build();
  }


  /**
   * WIP : exception handling not done properly yet
   * Run evaluation on all {@link DataFile} provided in separate threads and wait for the result before returning.
   *
   * @param sourceFileName
   * @param fileFormat
   * @param dataFiles
   * @return
   */
  private ValidationResult runEvaluation(String sourceFileName, FileFormat fileFormat, List<DataFile> dataFiles) {

    List<CompletableFuture<RecordsValidationResultElement>> completableEvaluations =
      dataFiles.stream().map(df -> CompletableFuture.supplyAsync(() ->  buildDataFileProcessor(df).process(df)))
                          .collect(Collectors.toList());

    //.exceptionally();

    CompletableFuture.allOf(completableEvaluations.toArray(new CompletableFuture[completableEvaluations.size()])).join();

    ValidationResultBuilders.Builder blrd = ValidationResultBuilders.Builder.of(true, sourceFileName,
                                                                                fileFormat, ValidationProfile.GBIF_INDEXING_PROFILE);
    completableEvaluations.forEach(e -> {
      try {
        blrd.withResourceResult(e.get());
      } catch (InterruptedException | ExecutionException ex) {
        LOG.error("Issue getting evaluation result ", ex);
      }
    });
    return blrd.build();
  }

  /**
   * Build a {@link DataFileProcessor} instance.
   *
   * @param dataFile
   * @return
   */
  private DataFileProcessor buildDataFileProcessor(@NotNull DataFile dataFile) {
    return new SingleDataFileProcessor(Arrays.asList(dataFile.getColumns()),
            factory.create(Arrays.asList(dataFile.getColumns()), dataFile.getRowType()),
            CollectorFactory.createInterpretedTermsCountCollector(dataFile.getRowType(), false));
  }

  /**
   * Build a {@link DataFileProcessor} instance configured to split source file(s).
   *
   * @param dataFile
   * @return
   */
  private DataFileProcessor buildDataFileProcessorWithSplit(@NotNull DataFile dataFile) {
    return buildParallelDataFileProcessor(dataFile);
  }

  private DataFileProcessor buildParallelDataFileProcessor(@NotNull DataFile dataFile) {
    return new ParallelDataFileProcessor(Arrays.asList(dataFile.getColumns()),
            factory.create(Arrays.asList(dataFile.getColumns()), dataFile.getRowType()),
            CollectorFactory.createInterpretedTermsCountCollector(dataFile.getRowType(), true), system, fileSplitSize,
            newJobId.getAndIncrement());
  }


}
