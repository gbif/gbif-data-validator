package org.gbif.validation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.RecordsValidationResultElement;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.single.SingleDataFileProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  private AtomicLong initialJobId;

  /**
   *
   * @param apiUrl
   * @param fileSplitSize threshold (in number of lines) until we use the parallel processing.
   */
  public ResourceEvaluationManager(String apiUrl, Integer fileSplitSize, ActorSystem system){
    factory = new EvaluatorFactory(apiUrl);
    this.fileSplitSize = fileSplitSize;
    this.system = system;
    initialJobId = new AtomicLong(new Date().getTime());
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
    Optional<ValidationResultElement> resourceStructureEvaluationResult =
      EvaluatorFactory.createResourceStructureEvaluator(dataFile.getFileFormat())
              .evaluate(dataFile.getFilePath(), dataFile.getSourceFileName());

    if(resourceStructureEvaluationResult.isPresent()) {
      return ValidationResultBuilders.Builder.of(false, dataFile.getSourceFileName(),
              dataFile.getFileFormat(), ValidationProfile.GBIF_INDEXING_PROFILE)
              .withResourceResult(resourceStructureEvaluationResult.get()).build();
    }

    //prepare the resource
    List<DataFile> preparedDataFiles = RecordSourceFactory.prepareSource(dataFile);

    int maxNumOfLine = preparedDataFiles.stream().mapToInt(df -> df.getNumOfLines()).max().getAsInt();

    if (maxNumOfLine <= fileSplitSize) {
      return runEvaluation(dataFile.getSourceFileName(), dataFile.getFileFormat(), preparedDataFiles);
    }

    return runEvaluationWithSplit(dataFile.getSourceFileName(), dataFile.getFileFormat(), preparedDataFiles);
  }

  private DataFileProcessor createDataFileProcessor(DataFile dataFile, List<Term> termsColumnsMapping) {
    if (dataFile.getNumOfLines() <= fileSplitSize) {
      //TODO create a Factory for Collectors
      InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(
              Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false);
      return new SingleDataFileProcessor(termsColumnsMapping, factory.create(termsColumnsMapping),
              interpretedTermsCountCollector);
    }
    return new ParallelDataFileProcessor(factory, system, termsColumnsMapping, fileSplitSize, initialJobId.incrementAndGet());
  /**
   * Split files and run evaluation on all of them.
   *
   * @param sourceFileName
   * @param fileFormat
   * @param dataFiles
   * @return
   */
  private ValidationResult runEvaluationWithSplit(String sourceFileName, FileFormat fileFormat,
                                                  List<DataFile> dataFiles) throws IOException {

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
            dataFiles.stream().map(df ->
                    CompletableFuture.supplyAsync(() -> {
                      try {
                        return buildDataFileProcessor(df).process(df);
                      } catch (IOException ioEx) {
                        LOG.error("Issue running evaluation on " + df.getFilePath(), ioEx);
                        //FIXME not a very good solution
                        throw new RuntimeException(ioEx);
                      }
                    }))
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
  //TODO create a Factory for Collectors
    Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector = DwcTerm.Occurrence == dataFile.getRowType() ?
            Optional.of(new InterpretedTermsCountCollector(
            Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false)):
            Optional.empty();

    return new SingleDataFileProcessor(Arrays.asList(dataFile.getColumns()),
            factory.create(Arrays.asList(dataFile.getColumns()), dataFile.getRowType()),
            interpretedTermsCountCollector);
  }

  /**
   * Build a {@link DataFileProcessor} instance configured to split source file(s).
   *
   * @param dataFile
   * @return
   */
  private DataFileProcessor buildDataFileProcessorWithSplit(@NotNull DataFile dataFile) {
    //TODO create a Factory for Collectors
    Optional<InterpretedTermsCountCollector> interpretedTermsCountCollector = DwcTerm.Occurrence == dataFile.getRowType() ?
            Optional.of(new InterpretedTermsCountCollector(
                    Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false)):
            Optional.empty();

    return new ParallelDataFileProcessor(Arrays.asList(dataFile.getColumns()),
            factory.create(Arrays.asList(dataFile.getColumns()), dataFile.getRowType()),
            interpretedTermsCountCollector, system, fileSplitSize);
  }

}
