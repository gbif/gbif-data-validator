package org.gbif.validation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.model.ValidationProfile;
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
import java.util.List;
import java.util.Optional;

import akka.actor.ActorSystem;

/**
 * {@link ResourceEvaluationManager} is responsible to create and trigger evaluations.
 *
 */
public class ResourceEvaluationManager {

  private final EvaluatorFactory factory;
  private final Integer fileSplitSize;

  private ActorSystem system;

  /**
   *
   * @param apiUrl
   * @param fileSplitSize threshold (in number of lines) until we use the parallel processing.
   */
  public ResourceEvaluationManager(String apiUrl, Integer fileSplitSize){
    factory = new EvaluatorFactory(apiUrl);
    this.fileSplitSize = fileSplitSize;
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

    // the list will handle extensions at some point
    if(preparedDataFiles.size() > 0) {
      List<Term> termsColumnsMapping = Arrays.asList(preparedDataFiles.get(0).getColumns());
      return createDataFileProcessor(preparedDataFiles.get(0), termsColumnsMapping).process(preparedDataFiles.get(0));
    }

    return null;
  }

  private DataFileProcessor createDataFileProcessor(DataFile dataFile, List<Term> termsColumnsMapping) {
    if (dataFile.getNumOfLines() <= fileSplitSize) {
      //TODO create a Factory for Collectors
      InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(
              Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false);
      return new SingleDataFileProcessor(termsColumnsMapping, factory.create(termsColumnsMapping),
              interpretedTermsCountCollector);
    }
    system = ActorSystem.create("DataFileProcessorSystem");
    return new ParallelDataFileProcessor(factory, system, termsColumnsMapping, fileSplitSize);
  }

}
