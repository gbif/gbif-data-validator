package org.gbif.validation.evaluator.runner;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.RecordSource;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.result.ValidationResult;
import org.gbif.validation.api.result.ValidationResultElement;
import org.gbif.validation.collector.CollectorGroup;
import org.gbif.validation.collector.CollectorGroupProvider;
import org.gbif.validation.evaluator.EvaluationChain;
import org.gbif.validation.evaluator.IndexableRules;
import org.gbif.validation.evaluator.ResourceConstitutionEvaluationChain;
import org.gbif.validation.source.RecordSourceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.api.model.ValidationProfile.GBIF_INDEXING_PROFILE;

/**
 * Collections of static functions to run evaluations.
 *
 */
public class LocalEvaluatorRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalEvaluatorRunner.class);

  /**
   * Utility class
   */
  private LocalEvaluatorRunner() {}

  /**
   * Run an {@link EvaluationChain} locally and sequentially.
   * Provides default functions to run all evaluations from an {@link EvaluationChain}.
   * @param ec
   * @return
   */
  public static ValidationResult run(final EvaluationChain ec) {
    final List<ValidationResultElement> results = new ArrayList<>();
    final Map<RowTypeKey, CollectorGroup> rowTypeCollectors = new HashMap<>();

    // ensure we the constitution of the DataFile is correct
    ResourceConstitutionEvaluationChain.ResourceConstitutionResult resourceConstitutionResults =  ec.runResourceConstitutionEvaluation();
    results.addAll(resourceConstitutionResults.getResults());

    if(resourceConstitutionResults.isEvaluationStopped()) {
      return buildValidationResultError(ec.getDataFile(), results);
    }

    ec.runDwcDataFileEvaluation((dataFile, metadataEvaluator) -> {
      run(dataFile, metadataEvaluator).ifPresent(r -> ValidationResultElement.mergeOnFilename(r, results));
    });
    ec.runRecordCollectionEvaluation((dwcDataFile, rowTypeKey, recordCollectionEvaluator) -> {
      rowTypeCollectors.putIfAbsent(rowTypeKey, new CollectorGroupProvider(rowTypeKey.getRowType(),
              Arrays.asList(dwcDataFile.getByRowTypeKey(rowTypeKey).getColumns())).newCollectorGroup());
      run(dwcDataFile, rowTypeKey, recordCollectionEvaluator, rowTypeCollectors.get(rowTypeKey));
    });
    try {
      ec.runRecordEvaluation((dataFiles, rowTypeKey, recordEvaluator) -> {
        dataFiles.forEach(df -> {
          rowTypeCollectors.putIfAbsent(rowTypeKey, new CollectorGroupProvider(rowTypeKey.getRowType(),
                  Arrays.asList(df.getColumns())).newCollectorGroup());
        });
        run(dataFiles, rowTypeKey, recordEvaluator, rowTypeCollectors.get(rowTypeKey));
      });
    } catch (IOException e) {
      e.printStackTrace();
    }

    for(TabularDataFile tdf : ec.getDwcDataFile().getTabularDataFiles()) {
      results.add(CollectorGroup.mergeAndGetResult(tdf, tdf.getSourceFileName(),
              Collections.singletonList(rowTypeCollectors.get(tdf.getRowTypeKey()))));
    }

    DataFile df = ec.getDataFile();
    return new ValidationResult(IndexableRules.isIndexable(results), df.getSourceFileName(),
            df.getFileFormat(), df.getReceivedAsMediaType(), GBIF_INDEXING_PROFILE, results);
  }

  private static ValidationResult buildValidationResultError(DataFile df, List<ValidationResultElement> results) {
    return new ValidationResult(false, df.getSourceFileName(),
            df.getFileFormat(), df.getReceivedAsMediaType(), GBIF_INDEXING_PROFILE, results);
  }

  static Optional<List<ValidationResultElement>> run(DwcDataFile dwcDataFile, DwcDataFileEvaluator metadataEvaluator) {
    return metadataEvaluator.evaluate(dwcDataFile);
  }

  /**
   * Matches RecordEvaluatorRunner functional interface.
   *
   * @param dataFiles
   * @param rowTypeKey
   * @param recordEvaluator
   */
  static void run(List<TabularDataFile> dataFiles, RowTypeKey rowTypeKey, RecordEvaluator recordEvaluator, CollectorGroup collectorGroup) {
    dataFiles.forEach(dataFile -> {
      try {
        runRecordEvaluator(dataFile, recordEvaluator, collectorGroup);
      } catch (Exception ex) { //catch everything to ensure we continue validation of other files
        LOG.error("Error while running record evaluation" , ex);
      }
    });
  }

  static void run(DwcDataFile dwcDataFile, RowTypeKey rowTypeKey, RecordCollectionEvaluator recordCollectionEvaluator,
                  CollectorGroup collectors) {
    try {
      recordCollectionEvaluator.evaluate(dwcDataFile, collectors::collectResult);
    } catch (IOException e) {
      collectors.collectResult(RecordEvaluationResult.Builder.of(rowTypeKey.getRowType(), (Long)null)
              .addBaseDetail(EvaluationType.UNHANDLED_ERROR, "", e.getMessage()).build());
    }
  }

  /**
   * Process a {@link TabularDataFile} by opening a {@link RecordSource} and evaluating all records
   * using the provided {@link RecordEvaluator}. All raw records and evaluation results are sent to the {@link CollectorGroup}.
   * On {@link IOException}, a {@link RecordEvaluationResult} will be added to the {@link CollectorGroup} and the exception
   * will be propagated.
   *
   * @param dataFile
   * @param recordEvaluator
   * @param collectors
   * @return
   */
  public static void runRecordEvaluator(TabularDataFile dataFile, RecordEvaluator recordEvaluator, CollectorGroup collectors) throws IOException {
    //add one if there is a header since the source will not send it
    long lineNumber = dataFile.getFileLineOffset().orElse(0) + (dataFile.isHasHeaders() ? + 1 : 0);
    //log().info("Starting to read: " + dataFile.getFilePath());
    try (RecordSource recordSource = RecordSourceFactory.fromTabularDataFile(dataFile)) {
      List<String> record;
      while ((record = recordSource.read()) != null) {
        //since files are normalized recordNumber == lineNumber
        lineNumber++;
        collectors.collectMetrics(record);
        collectors.collectResult(recordEvaluator.evaluate(lineNumber, record));
      }
    } catch (IOException ex) {
      // we collect the result to report the last line number
      collectors.collectResult(RecordEvaluationResult.Builder.of(dataFile.getRowTypeKey().getRowType(), lineNumber)
              .addBaseDetail(EvaluationType.UNREADABLE_SECTION_ERROR, "", ex.getMessage()).build());
      throw ex;
    }
  }
}
