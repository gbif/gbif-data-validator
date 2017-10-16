package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
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
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * The {@link EvaluationChain} is used to build and store the sequence of evaluation that will be performed.
 * An {@link EvaluationChain} is specific to each {@link DataFile} and they should NOT be reused.
 */
public class EvaluationChain {

  /**
   * Container class holding data between initialization and processing phase for {@link RecordEvaluator}.
   */
  private static class RecordEvaluationUnit {
    private final List<TabularDataFile> dataFiles;
    private final RowTypeKey rowTypeKey;
    private final RecordEvaluator recordEvaluator;

    RecordEvaluationUnit(List<TabularDataFile> dataFiles, RowTypeKey rowTypeKey, RecordEvaluator recordEvaluator) {
      this.dataFiles = dataFiles;
      this.rowTypeKey = rowTypeKey;
      this.recordEvaluator = recordEvaluator;
    }

    RecordEvaluator getRecordEvaluator() {
      return recordEvaluator;
    }

    RowTypeKey getRowTypeKey() {
      return rowTypeKey;
    }

    List<TabularDataFile> getDataFiles() {
      return dataFiles;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dataFiles", dataFiles)
              .add("rowTypeKey", rowTypeKey)
              .add("recordEvaluator", recordEvaluator)
              .toString();
    }
  }

  /**
   * Container class holding data between initialization and processing phase for {@link RecordCollectionEvaluator}.
   */
  private static class RowTypeEvaluationUnit {
    private final DwcDataFile dataFile;
    private final RowTypeKey rowTypeKey;
    private final RecordCollectionEvaluator recordCollectionEvaluator;

    RowTypeEvaluationUnit(DwcDataFile dataFile, RowTypeKey rowTypeKey,
                          RecordCollectionEvaluator recordCollectionEvaluator) {
      this.dataFile = dataFile;
      this.rowTypeKey = rowTypeKey;
      this.recordCollectionEvaluator = recordCollectionEvaluator;
    }

    public DwcDataFile getDataFile() {
      return dataFile;
    }

    public RowTypeKey getRowTypeKey() {
      return rowTypeKey;
    }

    public RecordCollectionEvaluator getRecordCollectionEvaluator() {
      return recordCollectionEvaluator;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dataFile", dataFile)
              .add("rowTypeKey", rowTypeKey)
              .add("recordCollectionEvaluator", recordCollectionEvaluator)
              .toString();
    }
  }

  /**
   * Container class holding data between initialization and processing phase for {@link DwcDataFileEvaluator}.
   */
  private static class DwcDataFileEvaluationUnit {
    private final DwcDataFile dwcDataFile;
    private final DwcDataFileEvaluator dwcDataFileEvaluator;

    DwcDataFileEvaluationUnit(DwcDataFile dwcDataFile, DwcDataFileEvaluator dwcDataFileEvaluator) {
      this.dwcDataFile = dwcDataFile;
      this.dwcDataFileEvaluator = dwcDataFileEvaluator;
    }

    public DwcDataFile getDwcDataFile() {
      return dwcDataFile;
    }

    public DwcDataFileEvaluator getDwcDataFileEvaluator() {
      return dwcDataFileEvaluator;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dwcDataFile", dwcDataFile)
              .add("dwcDataFileEvaluator", dwcDataFileEvaluator)
              .toString();
    }
  }

  /**
   * Builder class allowing to build an instance of {@link EvaluationChain}.
   */
  public static class Builder {
    private final DwcDataFile dwcDataFile;
    private final Path workingFolder;

    private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits = new ArrayList<>();
    private final List<RecordEvaluationUnit> recordEvaluationUnits = new ArrayList<>();
    private final List<DwcDataFileEvaluationUnit> dwcDataFileEvaluationUnits = new ArrayList<>();

    private final EvaluatorFactory factory;

    /**
     * Get a new {@link Builder} instance
     * @param dwcDataFile dataFile to use for validation
     * @param factory
     * @param workingFolder
     * @return new instance of {@link Builder}
     */
    public static Builder using(DwcDataFile dwcDataFile, EvaluatorFactory factory, Path workingFolder) {
      return new Builder(dwcDataFile, factory, workingFolder);
    }

//    public static Builder using(DataFile dataFile, DwcDataFileSupplier dwcDataFileSupplier,
//                                EvaluatorFactory factory, Path workingFolder) {
//      return new Builder(dwcDataFile, factory, workingFolder);
//    }

    private Builder(DwcDataFile dwcDataFile, EvaluatorFactory factory, Path workingFolder) {
      this.dwcDataFile = dwcDataFile;
      this.factory = factory;
      this.workingFolder = workingFolder;
    }

    /**
     * Add records evaluation as defined by the {@link EvaluatorFactory}.
     *
     * @return the builder
     */
    public Builder evaluateRecords() {
      Preconditions.checkState(recordEvaluationUnits.isEmpty(), "evaluateRecords shall only be called once");
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
      Preconditions.checkState(recordEvaluationUnits.isEmpty(), "evaluateRecords shall only be called once");

      for (TabularDataFile df : dwcDataFile.getTabularDataFiles()) {
        List<Term> columns = Arrays.asList(df.getColumns());
        recordEvaluationUnits.add(new RecordEvaluationUnit(transform.apply(df),
                df.getRowTypeKey(),
                factory.createRecordEvaluator(df.getRowTypeKey().getRowType(),
                        df.getRecordIdentifier().orElse(null), columns, df.getDefaultValues().orElse(null))));
      }
      return this;
    }

    public Builder evaluateCoreUniqueness() {
      rowTypeEvaluationUnits.add(new RowTypeEvaluationUnit(
              dwcDataFile, dwcDataFile.getCore().getRowTypeKey(),
              EvaluatorFactory.createUniquenessEvaluator(dwcDataFile.getCore().getRowTypeKey(), true, workingFolder)
      ));
      return this;
    }

    public Builder evaluateReferentialIntegrity() {
      //in case we have no extension, simply return
      if(!dwcDataFile.getExtensions().isPresent()){
        return this;
      }

      rowTypeEvaluationUnits.addAll(
              dwcDataFile.getExtensions().get().stream()
                      .map(df -> new RowTypeEvaluationUnit(
                              dwcDataFile, df.getRowTypeKey(),
                              EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowTypeKey().getRowType()))
                      )
                      .collect(Collectors.toList()));
      return this;
    }

    /**
     * Check the metadata content based on the default evaluator returned by the {@link EvaluatorFactory}.
     *
     * @return
     */
    public Builder evaluateMetadataContent() {
      dwcDataFileEvaluationUnits.add(new DwcDataFileEvaluationUnit(dwcDataFile, factory.createMetadataContentEvaluator()));
      return this;
    }

    public Builder evaluateChecklist() {
      List<TabularDataFile> taxonTabularDataFile = dwcDataFile.getByRowType(DwcTerm.Taxon);
      if (taxonTabularDataFile.isEmpty()) {
        return this;
      }

      for(TabularDataFile tfd : taxonTabularDataFile) {
        rowTypeEvaluationUnits.add(
                new RowTypeEvaluationUnit(
                        dwcDataFile, tfd.getRowTypeKey(),
                        factory.createChecklistEvaluator(workingFolder)
                ));
      }
      return this;
    }

    public EvaluationChain build() {
      return new EvaluationChain(dwcDataFile, dwcDataFileEvaluationUnits, rowTypeEvaluationUnits, recordEvaluationUnits);
    }
  }

  private final DwcDataFile dwcDataFile;
  private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits;
  private final List<RecordEvaluationUnit> recordEvaluationUnits;
  private final List<DwcDataFileEvaluationUnit> dwcDataFileEvaluationUnits;

  /**
   * Use {@link Builder}.
   *
   * @param dwcDataFileEvaluationUnits
   * @param rowTypeEvaluationUnits
   * @param recordEvaluationUnits
   */
  private EvaluationChain(DwcDataFile dwcDataFile, List<DwcDataFileEvaluationUnit> dwcDataFileEvaluationUnits,
                          List<RowTypeEvaluationUnit> rowTypeEvaluationUnits,
                          List<RecordEvaluationUnit> recordEvaluationUnits) {
    this.dwcDataFile = dwcDataFile;
    this.dwcDataFileEvaluationUnits = dwcDataFileEvaluationUnits;
    this.rowTypeEvaluationUnits = rowTypeEvaluationUnits;
    this.recordEvaluationUnits = recordEvaluationUnits;
  }

  /**
   *
   * @return should the evaluation chain continue or stop
   */
//  public boolean runResourceConstitutionEvaluation() {
//
//  }

  /**
   * Run all the {@link DwcDataFileEvaluator} using the provided {@link DwcDataFileEvaluatorRunner}.
   *
   * @param runner
   */
  public void runDwcDataFileEvaluation(DwcDataFileEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "DwcDataFileEvaluatorRunner shall be provided");
    dwcDataFileEvaluationUnits.forEach(unit -> runner.run(unit.getDwcDataFile(), unit.getDwcDataFileEvaluator()));
  }

  /**
   * Run all the {@link RecordCollectionEvaluator} using the provided {@link RecordCollectionEvaluatorRunner}.
   *
   * @param runner
   */
  public void runRecordCollectionEvaluation(RecordCollectionEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "RecordCollectionEvaluatorRunner shall be provided");
    rowTypeEvaluationUnits.forEach(unit -> runner.run(unit.getDataFile(),
            unit.getRowTypeKey(), unit.getRecordCollectionEvaluator()));
  }

  /**
   * Run all the {@link RecordEvaluator} using the provided {@link RecordEvaluatorRunner}.
   *
   * @param runner
   */
  public void runRecordEvaluation(RecordEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "RecordEvaluatorRunner shall be provided");
    recordEvaluationUnits.forEach(unit -> runner.run(unit.getDataFiles(),
            unit.getRowTypeKey(), unit.getRecordEvaluator()));
  }

  public int getNumberOfDwcDataFileEvaluationUnits() {
    return dwcDataFileEvaluationUnits.size();
  }

  public int getNumberOfRowTypeEvaluationUnits() {
    return rowTypeEvaluationUnits.size();
  }

  public int getNumberOfRecordEvaluationUnits() {
    return recordEvaluationUnits.size();
  }

  public DwcDataFile getDwcDataFile() {
    return dwcDataFile;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    if (!rowTypeEvaluationUnits.isEmpty()) {
      str.append("RowType EvaluationUnits:\n");
      rowTypeEvaluationUnits.stream().forEach(u -> str.append(u.toString() + "\n"));
    }
    if (!recordEvaluationUnits.isEmpty()) {
      str.append("Record EvaluationUnits:\n");
      recordEvaluationUnits.stream().forEach(u -> str.append(u.toString() + "\n"));
    }
    return str.toString();
  }

}
