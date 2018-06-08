package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.evaluator.runner.DwcDataFileEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordCollectionEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordEvaluatorRunner;
import org.gbif.validation.util.IOFunction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

/**
 * The {@link EvaluationChain} is used to build and store the sequence of evaluation that will be performed.
 * An {@link EvaluationChain} is specific to each {@link DataFile} and they should NOT be reused.
 */
public class EvaluationChain {

  private static class TargetedRecordCollectionEvaluator {
    private final RowTypeKey rowTypeKey;
    private final RecordCollectionEvaluator recordCollectionEvaluator;

    public TargetedRecordCollectionEvaluator(RowTypeKey rowTypeKey, RecordCollectionEvaluator recordCollectionEvaluator) {
      this.rowTypeKey = rowTypeKey;
      this.recordCollectionEvaluator = recordCollectionEvaluator;
    }
  }

  private static class TargetedRecordEvaluator {
    private final RowTypeKey rowTypeKey;
    private final TabularDataFile tabularDataFile;
    private final IOFunction<TabularDataFile, List<TabularDataFile>> transform;
    private final RecordEvaluator recordEvaluator;

    public TargetedRecordEvaluator(RowTypeKey rowTypeKey, TabularDataFile tabularDataFile,
                                   IOFunction<TabularDataFile, List<TabularDataFile>> transform, RecordEvaluator recordEvaluator) {
      this.rowTypeKey = rowTypeKey;
      this.tabularDataFile = tabularDataFile;
      this.transform = transform;
      this.recordEvaluator = recordEvaluator;
    }
  }

  /**
   * Builder class allowing to build an instance of {@link EvaluationChain}.
   */
  public static class Builder {
    //private final DwcDataFile dwcDataFile;
    private final DataFile dataFile;
    private final DwcDataFileSupplier dwcDataFileSupplier;
    private final Path workingFolder;

    private final List<Function<DwcDataFile, List<TargetedRecordCollectionEvaluator>>> recordCollectionEvaluatorFct = new ArrayList<>();
    private final List<Function<DwcDataFile, DwcDataFileEvaluator>> dwcDataFileEvaluationEvaluatorFct = new ArrayList<>();
    private final List<Function<DwcDataFile, List<TargetedRecordEvaluator>>> recordEvaluatorFct = new ArrayList<>();

    private final EvaluatorFactory factory;


    public static Builder using(DataFile dataFile, DwcDataFileSupplier dwcDataFileSupplier,
                                EvaluatorFactory factory, Path workingFolder) {
      return new Builder(dataFile, dwcDataFileSupplier, factory, workingFolder);
    }

    private Builder(DataFile dataFile, DwcDataFileSupplier dwcDataFileSupplier, EvaluatorFactory factory, Path workingFolder) {
      this.dataFile = dataFile;
      this.dwcDataFileSupplier = dwcDataFileSupplier;
      this.factory = factory;
      this.workingFolder = workingFolder;
    }

    /**
     * Add records evaluation as defined by the {@link EvaluatorFactory}.
     *
     * @return the builder
     */
    public Builder evaluateRecords() {
      Preconditions.checkState(recordEvaluatorFct.isEmpty(), "evaluateRecords shall only be called once");
      try {
        return evaluateRecords(Collections::singletonList);
      } catch (IOException ignore) {
        //safe to ignore, singletonList does not throw IOException
      }
      return null;
    }

    /**
     * Same as {@link #evaluateRecords()} but using a transform function to turn a single {@link TabularDataFile}
     * into a list of {@link TabularDataFile} (e.g. splitting a file into multiple smaller files)
     * @param transform
     * @return the builder
     */
    public Builder evaluateRecords(IOFunction<TabularDataFile, List<TabularDataFile>> transform) throws IOException {
      Preconditions.checkState(recordEvaluatorFct.isEmpty(), "evaluateRecords shall only be called once");

      recordEvaluatorFct.add((dwcDataFile) ->
        dwcDataFile.getTabularDataFiles().stream()
              .map( df -> new TargetedRecordEvaluator(df.getRowTypeKey(), df, transform,
                      factory.createRecordEvaluator(df.getRowTypeKey().getRowType(),
                          df.getRecordIdentifier().orElse(null), Arrays.asList(df.getColumns()),
                              df.getDefaultValues().orElse(null))))
                .collect(Collectors.toList()));

      return this;
    }

    public Builder evaluateCoreUniqueness() {
      recordCollectionEvaluatorFct.add((dwcDataFile) ->
              Collections.singletonList(
                      new TargetedRecordCollectionEvaluator(dwcDataFile.getCore().getRowTypeKey(),
                      EvaluatorFactory.createUniquenessEvaluator(dwcDataFile.getCore().getRowTypeKey(), true, workingFolder))));
      return this;
    }

    public Builder evaluateReferentialIntegrity() {
      recordCollectionEvaluatorFct.add((dwcDataFile) -> {
        if (dwcDataFile.getExtensions().isPresent()) {
          return dwcDataFile.getExtensions().get().stream()
                  .map(df -> new TargetedRecordCollectionEvaluator(df.getRowTypeKey(),
                          EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowTypeKey().getRowType())))
                  .collect(Collectors.toList());
        }
        return Collections.emptyList();
      });
      return this;
    }

    /**
     * Check the metadata content based on the default evaluator returned by the {@link EvaluatorFactory}.
     *
     * @return
     */
    public Builder evaluateMetadataContent() {
      dwcDataFileEvaluationEvaluatorFct.add((dwcDataFile) -> factory.createMetadataContentEvaluator());
      return this;
    }

    public Builder evaluateChecklist() {
      recordCollectionEvaluatorFct.add((dwcDataFile) -> {
        List<TabularDataFile> taxonTabularDataFile = dwcDataFile.getByRowType(DwcTerm.Taxon);
        return taxonTabularDataFile.stream().map(tdf -> new TargetedRecordCollectionEvaluator(tdf.getRowTypeKey(),
                factory.createChecklistEvaluator(workingFolder)))
                .collect(Collectors.toList());
      });
      return this;
    }

    public EvaluationChain build() {
      List<ResourceStructureEvaluator> resourceStructureEvaluators =
              Collections.singletonList(factory.createResourceStructureEvaluator(dataFile.getFileFormat()));
      List<DwcDataFileEvaluator> preRequisiteEvaluator = Collections.singletonList(factory.createPrerequisiteEvaluator());

      return new EvaluationChain(dataFile, new ResourceConstitutionEvaluationChain(dataFile, resourceStructureEvaluators,
              dwcDataFileSupplier, preRequisiteEvaluator),
              dwcDataFileEvaluationEvaluatorFct, recordCollectionEvaluatorFct, recordEvaluatorFct);
    }

  }

  private final DataFile dataFile;
  private final ResourceConstitutionEvaluationChain resourceConstitutionEvaluationChain;

  private final List<Function<DwcDataFile, DwcDataFileEvaluator>> dwcDataFileEvaluationEvaluatorFct;
  private final List<Function<DwcDataFile, List<TargetedRecordCollectionEvaluator>>> recordCollectionEvaluatorFct;
  private final List<Function<DwcDataFile, List<TargetedRecordEvaluator>>> recordEvaluatorFct;

  private ResourceConstitutionEvaluationChain.ResourceConstitutionResult resourceConstitutionResult;
  private DwcDataFile dwcDataFile;

  /**
   * Use {@link Builder}.
   *
   */
  private EvaluationChain(DataFile dataFile, ResourceConstitutionEvaluationChain resourceConstitutionEvaluationChain,
                          List<Function<DwcDataFile, DwcDataFileEvaluator>> dwcDataFileEvaluationEvaluatorFct,
                          List<Function<DwcDataFile, List<TargetedRecordCollectionEvaluator>>> recordCollectionEvaluatorFct,
                          List<Function<DwcDataFile, List<TargetedRecordEvaluator>>> recordEvaluatorFct) {
    this.dataFile = dataFile;
    this.resourceConstitutionEvaluationChain = resourceConstitutionEvaluationChain;
    this.dwcDataFileEvaluationEvaluatorFct = dwcDataFileEvaluationEvaluatorFct;
    this.recordCollectionEvaluatorFct = recordCollectionEvaluatorFct;
    this.recordEvaluatorFct = recordEvaluatorFct;
  }

  /**
   *
   * @return should the evaluation chain continue or stop
   */
  public ResourceConstitutionEvaluationChain.ResourceConstitutionResult runResourceConstitutionEvaluation() {
    resourceConstitutionResult = resourceConstitutionEvaluationChain.run();
    if(!resourceConstitutionResult.isEvaluationStopped()) {
      dwcDataFile = resourceConstitutionResult.getTransformedDataFile();
    }
    return resourceConstitutionResult;
  }

  /**
   * Run all the {@link DwcDataFileEvaluator} using the provided {@link DwcDataFileEvaluatorRunner}.
   *
   * @param runner
   */
  public void runDwcDataFileEvaluation(DwcDataFileEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "DwcDataFileEvaluatorRunner shall be provided");
    Objects.requireNonNull(resourceConstitutionResult, "runResourceConstitutionEvaluation shall be called before runRecordEvaluation");
    dwcDataFileEvaluationEvaluatorFct.forEach(fct -> runner.run(dwcDataFile, fct.apply(dwcDataFile)));
  }

  /**
   * Run all the {@link RecordCollectionEvaluator} using the provided {@link RecordCollectionEvaluatorRunner}.
   *
   * @param runner
   */
  public void runRecordCollectionEvaluation(RecordCollectionEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "RecordCollectionEvaluatorRunner shall be provided");
    Objects.requireNonNull(resourceConstitutionResult, "runResourceConstitutionEvaluation shall be called before runRecordEvaluation");

    recordCollectionEvaluatorFct.forEach(fct -> {
      fct.apply(dwcDataFile).forEach(trce -> runner.run(dwcDataFile, trce.rowTypeKey, trce.recordCollectionEvaluator));
    });
  }

  /**
   * Run all the {@link RecordEvaluator} using the provided {@link RecordEvaluatorRunner}.
   *
   * @param runner
   */
  public void runRecordEvaluation(RecordEvaluatorRunner runner) throws IOException {
    Objects.requireNonNull(runner, "RecordEvaluatorRunner shall be provided");
    Objects.requireNonNull(resourceConstitutionResult, "runResourceConstitutionEvaluation shall be called before runRecordEvaluation");

    for(Function<DwcDataFile, List<TargetedRecordEvaluator>> fct : recordEvaluatorFct) {
      for(TargetedRecordEvaluator tre : fct.apply(dwcDataFile)){
        runner.run(tre.transform.apply(tre.tabularDataFile), tre.rowTypeKey, tre.recordEvaluator);
      }
    }
  }

  public DataFile getDataFile() {
    return dataFile;
  }

  public DwcDataFile getDwcDataFile() {
    return dwcDataFile;
  }

}
