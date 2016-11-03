package org.gbif.validation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.ValidationProfile;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultBuilders;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.single.SingleDataFileProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import akka.actor.ActorSystem;

/**
 *
 */
public class ResourceEvaluationManager {

  public static final int FILE_SPLIT_SIZE = 10000;
  private final EvaluatorFactory factory;
  private ActorSystem system;

  public ResourceEvaluationManager(String apiUrl){
    factory = new EvaluatorFactory(apiUrl);
  }

  public ValidationResult evaluate(DataFile dataFile) throws IOException {
    Optional<ValidationResultElement> resourceStructureEvaluationResult = factory.createResourceStructureEvaluator(dataFile.getFileFormat()).evaluate(
            new File(dataFile.getFileName()).toPath(), dataFile.getSourceFileName());

    if(resourceStructureEvaluationResult.isPresent()) {
      return ValidationResultBuilders.Builder.of(false, dataFile.getSourceFileName(),
              FileFormat.TABULAR, ValidationProfile.GBIF_INDEXING_PROFILE)
              .withResourceResult(resourceStructureEvaluationResult.get()).build();
    }

    DataFile preparedDataFile = RecordSourceFactory.prepareSource(dataFile);
    List<Term> termsColumnsMapping = Arrays.asList(preparedDataFile.getColumns());

    return createDataFileProcessor(dataFile, termsColumnsMapping).process(dataFile);
  }

  private DataFileProcessor createDataFileProcessor(DataFile dataFile, List<Term> termsColumnsMapping) {
    if (dataFile.getNumOfLines() <= FILE_SPLIT_SIZE) {
      //TODO create a Factory for Collectors
      InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(
              Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false);
      return new SingleDataFileProcessor(termsColumnsMapping, factory.create(termsColumnsMapping),
              interpretedTermsCountCollector);
    }
    system = ActorSystem.create("DataFileProcessorSystem");
    return new ParallelDataFileProcessor(factory, system, termsColumnsMapping);
  }


}
