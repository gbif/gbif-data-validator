package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.occurrence.validation.evaluator.OccurrenceEvaluatorFactory;
import org.gbif.occurrence.validation.tabular.single.SingleDataFileProcessor;

/**
 * Creates instances of DataFile processors.
 */
public class OccurrenceDataFileProcessorFactory {

  public static final int FILE_SPLIT_SIZE = 10000;

  private final String apiUrl;

  /**
   * Default constructor.
   * @param apiUrl url to the GBIF api.
   */
  public OccurrenceDataFileProcessorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * Creates a DataFileProcessor instance analyzing the size of the input file.
   * If the file exceeds certain size it's processed in parallel otherwise a single thread processor it's used.
   */
  public DataFileProcessor create(DataFile dataFile) {
    OccurrenceEvaluatorFactory factory = new OccurrenceEvaluatorFactory(apiUrl);

    if (dataFile.getNumOfLines() <= FILE_SPLIT_SIZE) {
      return new SingleDataFileProcessor(factory.create(dataFile.getColumns()));
    }
    return new ParallelDataFileProcessor(apiUrl);
  }

}
