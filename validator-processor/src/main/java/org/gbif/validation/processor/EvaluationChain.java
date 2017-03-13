package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.MetadataEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.evaluator.runner.MetadataEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordCollectionEvaluatorRunner;
import org.gbif.validation.evaluator.runner.RecordEvaluatorRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
   * Container class holding data between initialization and processing phase for {@link MetadataEvaluator}.
   */
  private static class MetadataEvaluationUnit {
    private final DwcDataFile dwcDataFile;
    private final MetadataEvaluator metadataEvaluator;

    MetadataEvaluationUnit(DwcDataFile dwcDataFile, MetadataEvaluator metadataEvaluator) {
      this.dwcDataFile = dwcDataFile;
      this.metadataEvaluator = metadataEvaluator;
    }

    public DwcDataFile getDwcDataFile() {
      return dwcDataFile;
    }

    public MetadataEvaluator getMetadataEvaluator() {
      return metadataEvaluator;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("dwcDataFile", dwcDataFile)
              .add("metadataEvaluator", metadataEvaluator)
              .toString();
    }
  }

  /**
   * Builder class allowing to build an instance of {@link EvaluationChain}.
   */
  public static class Builder {
    private final DwcDataFile dwcDataFile;

    private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits = new ArrayList<>();
    private final List<RecordEvaluationUnit> recordEvaluationUnits = new ArrayList<>();
    private final List<MetadataEvaluationUnit> metadataEvaluationUnits = new ArrayList<>();

    private final EvaluatorFactory factory;

    /**
     *
     * @param dwcDataFile dataFile received for validation
     * @param factory
     * @return new instance of {@link Builder}
     */
    public static Builder using(DwcDataFile dwcDataFile, EvaluatorFactory factory) {
      return new Builder(dwcDataFile, factory);
    }

    private Builder(DwcDataFile dwcDataFile, EvaluatorFactory factory) {
      this.dwcDataFile = dwcDataFile;
      this.factory = factory;
    }

    /**
     * FIXME we can take the columns and defaultValues from the dataFiles variable.
     * @param dataFile all the same rowType
     * @return
     */
    public Builder evaluateRecords(Term rowType, List<Term> columns, Optional<Map<Term, String>> defaultValues,
                                   List<TabularDataFile> dataFile) {
      recordEvaluationUnits.add(new RecordEvaluationUnit(dataFile, rowType,
              factory.create(rowType, columns, defaultValues)));
      return this;
    }

    public Builder evaluateCoreUniqueness() {
      rowTypeEvaluationUnits.add(new RowTypeEvaluationUnit(
              dwcDataFile, dwcDataFile.getCore().getRowType(),
              EvaluatorFactory.createUniquenessEvaluator(dwcDataFile.getCore().getRowType(), true)
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

    public Builder evaluateMetadataContent(){
      metadataEvaluationUnits.add(new MetadataEvaluationUnit(dwcDataFile, factory.createMetadataContentEvaluator()));
      return this;
    }

    public Builder evaluateChecklist() {

      if (dwcDataFile.getByRowType(DwcTerm.Taxon) == null) {
        return this;
      }
      rowTypeEvaluationUnits.add(
              new RowTypeEvaluationUnit(
                      dwcDataFile, DwcTerm.Taxon,
                      factory.createChecklistEvaluator()
              ));
      return this;
    }

    public EvaluationChain build() {
      return new EvaluationChain(metadataEvaluationUnits, rowTypeEvaluationUnits, recordEvaluationUnits);
    }
  }

  private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits;
  private final List<RecordEvaluationUnit> recordEvaluationUnits;
  private final List<MetadataEvaluationUnit> metadataEvaluationUnits;

  /**
   * Use {@link Builder}.
   *
   * @param metadataEvaluationUnits
   * @param rowTypeEvaluationUnits
   * @param recordEvaluationUnits
   */
  private EvaluationChain(List<MetadataEvaluationUnit> metadataEvaluationUnits,
                          List<RowTypeEvaluationUnit> rowTypeEvaluationUnits,
                          List<RecordEvaluationUnit> recordEvaluationUnits) {
    this.metadataEvaluationUnits = metadataEvaluationUnits;
    this.rowTypeEvaluationUnits = rowTypeEvaluationUnits;
    this.recordEvaluationUnits = recordEvaluationUnits;
  }

  /**
   * Run all the {@link MetadataEvaluator} using the provided {@link MetadataEvaluatorRunner}.
   *
   * @param runner
   */
  public void runMetadataContentEvaluation(MetadataEvaluatorRunner runner) {
    Objects.requireNonNull(runner, "MetadataEvaluatorRunner shall be provided");
    metadataEvaluationUnits.forEach(unit -> runner.run(unit.getDwcDataFile(), unit.getMetadataEvaluator()));
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

  public int getNumberOfMetadataEvaluationUnits() {
    return metadataEvaluationUnits.size();
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
