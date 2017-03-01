package org.gbif.validation.processor;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.TabularDataFile;
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

    @Override
    public String toString() {
      return "dataFiles:{" + dataFiles + "}," +
              "rowType: " + rowType + ", " +
              "recordEvaluator: " + recordEvaluator;
    }
  }

  /**
   * Container class holding data between initialization and processing phase for {@link RecordCollectionEvaluator}.
   */
  public static class RowTypeEvaluationUnit {
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

    public Optional<Stream<RecordEvaluationResult>>  evaluate() throws IOException {
      return recordCollectionEvaluator.evaluate(dataFile);
    }

    public RecordCollectionEvaluator getRecordCollectionEvaluator() {
      return recordCollectionEvaluator;
    }

    @Override
    public String toString() {
      return "dataFile: " + dataFile + "}," +
              "rowType: " + rowType + ", " +
              "recordCollectionEvaluator: " + recordCollectionEvaluator;
    }
  }

  /**
   * Builder class allowing to build an instance of {@link EvaluationChain}.
   */
  public static class Builder {
    private final DwcDataFile dwcDataFile;

    private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits = new ArrayList<>();
    private final List<RecordEvaluationUnit> recordEvaluationUnits = new ArrayList<>();

    private final EvaluatorFactory factory;

    /**
     *
     * @param dwcDataFile dataFile received for validation
     * @param factory
     * @return
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
      return new EvaluationChain(rowTypeEvaluationUnits, recordEvaluationUnits);
    }
  }

  private final List<RowTypeEvaluationUnit> rowTypeEvaluationUnits;
  private final List<RecordEvaluationUnit> recordEvaluationUnits;

  private EvaluationChain(List<RowTypeEvaluationUnit> rowTypeEvaluationUnits,
                          List<RecordEvaluationUnit> recordEvaluationUnits) {
    this.rowTypeEvaluationUnits = rowTypeEvaluationUnits;
    this.recordEvaluationUnits = recordEvaluationUnits;
  }

  /**
   * Run the provided function on all RowTypeEvaluationUnit in the evaluation chain.
   * @param fct
   */
  public void runRowTypeEvaluation(Consumer<RowTypeEvaluationUnit> fct) {
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
