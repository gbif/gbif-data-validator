package org.gbif.validation.tabular;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.collector.InterpretedTermsCountCollector;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.source.RecordSourceFactory;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.single.SingleDataFileProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import akka.actor.ActorSystem;

/**
 * Creates instances of DataFile processors.
 */
public class DataFileProcessorFactory {

  public static final int FILE_SPLIT_SIZE = 10000;

   private final EvaluatorFactory factory;

  private ActorSystem system;

  /**
   * Default constructor.
   * @param apiUrl url to the GBIF api.
   */
  public DataFileProcessorFactory(String apiUrl) {
    factory = new EvaluatorFactory(apiUrl);
    // Create an Akka system
    system = ActorSystem.create("DataFileProcessorSystem");
  }

  /**
   * Creates a DataFileProcessor instance analyzing the size of the input file.
   * If the file exceeds certain size it's processed in parallel otherwise a single thread processor it's used.
   */
  public DataFileProcessor create(DataFile dataFile) throws IOException {
    DataFile preparedDataFile = RecordSourceFactory.prepareSource(dataFile);
    List<Term> termsColumnsMapping = Arrays.asList(preparedDataFile.getColumns());

    if (dataFile.getNumOfLines() <= FILE_SPLIT_SIZE) {
      //TODO create a Factory for Collectors
      InterpretedTermsCountCollector interpretedTermsCountCollector = new InterpretedTermsCountCollector(
              Arrays.asList(DwcTerm.year, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.taxonKey), false);
      return new SingleDataFileProcessor(termsColumnsMapping, factory.create(termsColumnsMapping), interpretedTermsCountCollector);
    }
    return new ParallelDataFileProcessor(factory, system, termsColumnsMapping);
  }

}
