package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.evaluator.runner.DwcDataFileEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordCollectionEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordEvaluatorRunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;

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
    private final Term rowType;
    private final RecordEvaluator recordEvaluator;

    RecordEvaluationUnit(List<TabularDataFile> dataFiles, Term rowType, RecordEvaluator recordEvaluator) {
      this.dataFiles = dataFiles;
      this.rowType = rowType;
      this.recordEvaluator = recordEvaluator;
    }

    public RecordEvaluator getRecordEvaluator() {
      return recordEvaluator;
    }

    public Term getRowType() {
      return rowType;
    }

    public List<TabularDataFile> getDataFiles() {
      return dataFiles;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dataFiles", dataFiles)
              .add("rowType", rowType)
              .add("recordEvaluator", recordEvaluator)
              .toString();
    }
  }

  /**
   * Container class holding data between initialization and processing phase for {@link RecordCollectionEvaluator}.
   */
  private static class RowTypeEvaluationUnit {
    private final DwcDataFile dataFile;
    private final Term rowType;
    private final RecordCollectionEvaluator recordCollectionEvaluator;

    RowTypeEvaluationUnit(DwcDataFile dataFile, Term rowType,
                          RecordCollectionEvaluator recordCollectionEvaluator) {
      this.dataFile = dataFile;
      this.rowType = rowType;
      this.recordCollectionEvaluator = recordCollectionEvaluator;
    }

    public DwcDataFile getDataFile() {
      return dataFile;
    }

    public Term getRowType() {
      return rowType;
    }

    public RecordCollectionEvaluator getRecordCollectionEvaluator() {
      return recordCollectionEvaluator;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dataFile", dataFile)
              .add("rowType", rowType)
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
     *
     * @param dwcDataFile dataFile received for validation
     * @param factory
     * @return new instance of {@link Builder}
     */
    public static Builder using(DwcDataFile dwcDataFile, EvaluatorFactory factory, Path workingFolder) {
      return new Builder(dwcDataFile, factory, workingFolder);
    }

    private Builder(DwcDataFile dwcDataFile, EvaluatorFactory factory, Path workingFolder) {
      this.dwcDataFile = dwcDataFile;
      this.factory = factory;
      this.workingFolder = workingFolder;
    }

    /**
     * FIXME we can take the columns and defaultValues from the dataFiles variable.
     * @param dataFile all the same rowType
     * @return
     */
    public Builder evaluateRecords(Term rowType, List<Term> columns, Map<Term, String> defaultValues,
                                   List<TabularDataFile> dataFile) {
      recordEvaluationUnits.add(new RecordEvaluationUnit(dataFile, rowType,
              factory.create(rowType, columns, defaultValues)));
      return this;
    }

    public Builder evaluateCoreUniqueness() {
      rowTypeEvaluationUnits.add(new RowTypeEvaluationUnit(
              dwcDataFile, dwcDataFile.getCore().getRowType(),
              EvaluatorFactory.createUniquenessEvaluator(dwcDataFile.getCore().getRowType(), true, workingFolder)
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
                              dwcDataFile, df.getRowType(),
                              EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowType()))
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

      if (dwcDataFile.getByRowType(DwcTerm.Taxon) == null) {
        return this;
      }
      rowTypeEvaluationUnits.add(
              new RowTypeEvaluationUnit(
                      dwcDataFile, DwcTerm.Taxon,
                      factory.createChecklistEvaluator(workingFolder)
              ));
      return this;
    }

    public EvaluationChain build() {
      return new EvaluationChain(dwcDataFileEvaluationUnits, rowTypeEvaluationUnits, recordEvaluationUnits);
    }
  }

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
  private EvaluationChain(List<DwcDataFileEvaluationUnit> dwcDataFileEvaluationUnits,
                          List<RowTypeEvaluationUnit> rowTypeEvaluationUnits,
                          List<RecordEvaluationUnit> recordEvaluationUnits) {
    this.dwcDataFileEvaluationUnits = dwcDataFileEvaluationUnits;
    this.rowTypeEvaluationUnits = rowTypeEvaluationUnits;
    this.recordEvaluationUnits = recordEvaluationUnits;
  }

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
  public void runRowTypeEvaluation(RecordCollectionEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "RecordCollectionEvaluatorRunner shall be provided");
    rowTypeEvaluationUnits.forEach(unit -> runner.run(unit.getDataFile(),
            unit.getRowType(), unit.getRecordCollectionEvaluator()));
  }

  /**
   * Run all the {@link RecordEvaluator} using the provided {@link RecordEvaluatorRunner}.
   *
   * @param runner
   */
  public void runRecordEvaluation(RecordEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "RecordEvaluatorRunner shall be provided");
    recordEvaluationUnits.forEach(unit -> runner.run(unit.getDataFiles(),
            unit.getRowType(), unit.getRecordEvaluator()));
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
