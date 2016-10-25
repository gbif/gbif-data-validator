package org.gbif.validation.tabular;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DataFileProcessor;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.single.SingleDataFileProcessor;

import akka.actor.ActorSystem;

import static org.gbif.validation.util.TempTermsUtils.buildTermMapping;

/**
 * Creates instances of DataFile processors.
 */
public class OccurrenceDataFileProcessorFactory {

  public static final int FILE_SPLIT_SIZE = 10000;

  private final String apiUrl;

  private ActorSystem system;

  /**
   * Default constructor.
   * @param apiUrl url to the GBIF api.
   */
  public OccurrenceDataFileProcessorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
    // Create an Akka system
    system = ActorSystem.create("DataFileProcessorSystem");
  }

  /**
   * Creates a DataFileProcessor instance analyzing the size of the input file.
   * If the file exceeds certain size it's processed in parallel otherwise a single thread processor it's used.
   */
  public DataFileProcessor create(DataFile dataFile) {
    EvaluatorFactory factory = new EvaluatorFactory(apiUrl);
    Term[] termsColumnsMapping = buildTermMapping(dataFile.getColumns());

    if (dataFile.getNumOfLines() <= FILE_SPLIT_SIZE) {
      return new SingleDataFileProcessor(termsColumnsMapping, factory.create(termsColumnsMapping));
    }
    return new ParallelDataFileProcessor(apiUrl, system, termsColumnsMapping);
  }

}
