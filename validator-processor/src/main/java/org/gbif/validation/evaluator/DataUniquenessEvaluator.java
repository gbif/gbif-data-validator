package org.gbif.validation.evaluator;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import g14.com.google.common.collect.ImmutableMap;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.util.FileBashUtilities;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;

/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the uniqueness of data identifiers
 * in a {@link TabularDataFile}
 *
 * For example, in an Occurrence archive the term occurrenceID must be unique.
 */
class DataUniquenessEvaluator implements RecordCollectionEvaluator {

  private org.gbif.utils.file.FileUtils GBIF_FILE_UTILS = new org.gbif.utils.file.FileUtils();

  private final Path workingFolder;

  public DataUniquenessEvaluator(boolean ignoreCase, Path workingFolder) {
    Preconditions.checkArgument(!ignoreCase, "Case-insensitive check isn't yet supported.");

    this.workingFolder = workingFolder;
  }

  @Override
  public void evaluate(@NotNull DwcDataFile dwcDataFile, Consumer<RecordEvaluationResult> resultConsumer) throws IOException {

    RowTypeKey coreTypeKey = dwcDataFile.getCore().getRowTypeKey();
    if (RowTypeKey.forCore(DwcTerm.Occurrence).equals(coreTypeKey)) {
      // Occurrence core, check occurrenceID is unique in the core
      TabularDataFile dataFile = dwcDataFile.getCore();
      OptionalInt index = dataFile.getIndexOf(DwcTerm.occurrenceID);
      if (index.isPresent()) {
        doEvaluate(DwcTerm.occurrenceID, dataFile, resultConsumer);
      }

    } else if (RowTypeKey.forCore(DwcTerm.Event).equals(coreTypeKey)) {
      // Event core, check occurrence extension occurrenceIDs.
      TabularDataFile dataFile = dwcDataFile.getByRowTypeKey(RowTypeKey.forExtension(DwcTerm.Occurrence));
      OptionalInt index = dataFile.getIndexOf(DwcTerm.occurrenceID);
      if (index.isPresent()) {
        doEvaluate(DwcTerm.occurrenceID, dataFile, resultConsumer);
      }
    } else if (RowTypeKey.forCore(DwcTerm.Taxon).equals(coreTypeKey)) {
      // Checklist core, could check TypesAndExtensions.
    }
  }

  private void doEvaluate(Term uniqueTerm, TabularDataFile dataFile, Consumer<RecordEvaluationResult> resultConsumer) throws IOException {
    Preconditions.checkState(dataFile != null && dataFile.getRecordIdentifier().isPresent(),
      "DwcDataFile {} shall have a record identifier", dataFile.getRowTypeKey());


    int termColumnIndex = dataFile.getIndexOf(DwcTerm.occurrenceID).orElse(-1);

    int keyColumnIndex = dataFile.getRecordIdentifier().get().getIndex();

    File sourceFile = dataFile.getFilePath().toFile();
    File sortedFile = workingFolder.resolve(sourceFile.getName() + "_sorted").toFile();

    GBIF_FILE_UTILS.sort(sourceFile, sortedFile, Charsets.UTF_8.toString(), termColumnIndex,
            dataFile.getDelimiterChar().toString(), dataFile.getQuoteChar(), "\n", dataFile.isHasHeaders() ? 1 : 0);

    //FIXME doesn't support case insensitive for now
    List<String[]> result = FileBashUtilities.findDuplicates(sortedFile.getAbsolutePath(), keyColumnIndex + 1,
      termColumnIndex + 1, dataFile.getDelimiterChar().toString(), dataFile.getQuoteChar(), dataFile.getCharacterEncoding());

    result.forEach(rec -> resultConsumer.accept(buildResult(dataFile.getRowTypeKey(), uniqueTerm, rec[0], rec[1])));
  }

  private static RecordEvaluationResult buildResult(RowTypeKey rowTypeKey, Term fieldType, String id, String nonUniqueValue){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED, ImmutableMap.of(fieldType, nonUniqueValue)));

    return new RecordEvaluationResult(rowTypeKey.getRowType(), null,  id, resultDetails, null, null);
  }
}
