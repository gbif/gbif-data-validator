package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.DwcFileType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.evaluator.EvaluatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link EvaluationChain} is used to build and store the sequence of evaluation that will be performed.
 * An {@link EvaluationChain} is specific to each {@link DataFile} and they should NOT be reused.
 */
public class EvaluationChain {

  /**
   * Container class holding data between initialization and processing phase for {@link RecordEvaluator}.
   */
  public static class RecordEvaluationUnit {
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
  }

  /**
   * Container class holding data between initialization and processing phase for {@link RecordCollectionEvaluator}.
   */
  public static class RowTypeEvaluationUnit<T extends DataFile> {
    private final T dataFile;
    private final Term rowType;
    private final RecordCollectionEvaluator<T> recordCollectionEvaluator;

    RowTypeEvaluationUnit(T dataFile, Term rowType,
                          RecordCollectionEvaluator<T>  recordCollectionEvaluator) {
      this.dataFile = dataFile;
      this.rowType = rowType;
      this.recordCollectionEvaluator = recordCollectionEvaluator;
    }

    public T getDataFile() {
      return dataFile;
    }

    public Term getRowType() {
      return rowType;
    }

    public Optional<Stream<RecordEvaluationResult>>  evaluate() throws IOException {
      return recordCollectionEvaluator.evaluate(dataFile);
    }

    public RecordCollectionEvaluator<T> getRecordCollectionEvaluator() {
      return recordCollectionEvaluator;
    }
  }

  /**
   * Builder class allowing to build an instance of {@link EvaluationChain}.
   */
  public static class Builder {
    private final DataFile sourceDataFile;

    private final TabularDataFile coreDataFile;
    private final List<TabularDataFile> dataFiles;

    private final List<RowTypeEvaluationUnit<? extends DataFile>> rowTypeEvaluationUnits = new ArrayList<>();
    private final List<RecordEvaluationUnit> recordEvaluationUnits = new ArrayList<>();

    private final Map<DwcFileType, List<TabularDataFile>> dfPerRowType;
    private final EvaluatorFactory factory;

    /**
     *
     * @param sourceDataFile dataFile received for validation
     * @param dataFiles
     * @param factory
     * @return
     */
    public static Builder using(DataFile sourceDataFile, List<TabularDataFile> dataFiles, EvaluatorFactory factory) {
      return new Builder(sourceDataFile, dataFiles, factory);
    }

    private Builder(DataFile sourceDataFile, List<TabularDataFile> dataFiles, EvaluatorFactory factory) {
      this.sourceDataFile = sourceDataFile;
      this.dataFiles = dataFiles;

      dfPerRowType = dataFiles.stream()
              .collect(Collectors.groupingBy(TabularDataFile::getType));
      //we assume the core file is there
      this.coreDataFile = dfPerRowType.get(DwcFileType.CORE).get(0);
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
              coreDataFile, coreDataFile.getRowType(),
              EvaluatorFactory.createUniquenessEvaluator(coreDataFile.getIndexOf(coreDataFile.getRecordIdentifier().get()).getAsInt() + 1, true)
      ));
      return this;
    }

    public Builder evaluateReferentialIntegrity() {
      rowTypeEvaluationUnits.addAll(
              dfPerRowType.get(DwcFileType.EXTENSION).stream()
                      .map(df -> new RowTypeEvaluationUnit<>(
                              sourceDataFile, df.getRowType(),
                              EvaluatorFactory.createReferentialIntegrityEvaluator(df.getRowType()))
                      )
                      .collect(Collectors.toList()));
      return this;
    }

    public Builder evaluateChecklist() {
      dataFiles.stream()
              .filter(e -> DwcTerm.Taxon.equals(e.getRowType()))
              .findFirst()
              .ifPresent(taxonDataFile ->
                      rowTypeEvaluationUnits.add(
                              new RowTypeEvaluationUnit(
                                      sourceDataFile, DwcTerm.Taxon,
                                      factory.createChecklistEvaluator()
                              )));
      return this;
    }

    public EvaluationChain build() {
      return new EvaluationChain(rowTypeEvaluationUnits, recordEvaluationUnits);
    }
  }

  private final List<RowTypeEvaluationUnit<? extends DataFile>> rowTypeEvaluationUnits;
  private final List<RecordEvaluationUnit> recordEvaluationUnits;

  private EvaluationChain(List<RowTypeEvaluationUnit<? extends DataFile>> rowTypeEvaluationUnits,
                          List<RecordEvaluationUnit> recordEvaluationUnits) {
    this.rowTypeEvaluationUnits = rowTypeEvaluationUnits;
    this.recordEvaluationUnits = recordEvaluationUnits;
  }

  /**
   * Run the provided function on all RowTypeEvaluationUnit in the evaluation chain.
   * @param fct
   */
  public void runRowTypeEvaluation(Consumer<RowTypeEvaluationUnit<? extends DataFile>> fct) {
    rowTypeEvaluationUnits.forEach(fct);
  }

  /**
   * Run the provided function on all RecordEvaluationUnit in the evaluation chain.
   * @param fct
   */
  public void runRecordEvaluation(Consumer<RecordEvaluationUnit> fct) {
    recordEvaluationUnits.forEach(fct);
  }

  public int getNumberOfRowTypeEvaluationUnits() {
    return rowTypeEvaluationUnits.size();
  }

  public int getNumberOfRecordEvaluationUnits() {
    return recordEvaluationUnits.size();
  }


}
