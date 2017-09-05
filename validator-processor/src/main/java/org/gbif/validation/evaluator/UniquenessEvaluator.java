package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the uniqueness of the records identifier
 * in a {@link TabularDataFile}
 */
class UniquenessEvaluator implements RecordCollectionEvaluator {

  private org.gbif.utils.file.FileUtils GBIF_FILE_UTILS = new org.gbif.utils.file.FileUtils();

  private final Term rowType;
  private final boolean caseSensitive;
  private final Path workingFolder;

  /**
   *
   * @param rowType Term used as identifier for the dataFile (start at 1)
   */
  public UniquenessEvaluator(Term rowType, boolean caseSensitive, Path workingFolder) {
    this.rowType = rowType;
    this.caseSensitive = caseSensitive;
    this.workingFolder = workingFolder;
  }

  @Override
  public void evaluate(@NotNull DwcDataFile dwcDataFile, Consumer<RecordEvaluationResult> resultConsumer) throws IOException {

    TabularDataFile dataFile = dwcDataFile.getByRowType(rowType);
    Preconditions.checkState(dataFile != null && dataFile.getRecordIdentifier().isPresent(),
            "DwcDataFile {} shall have a record identifier", rowType);
    int idColumnIndex = dataFile.getRecordIdentifier().get().getIndex();

    File sourceFile = dataFile.getFilePath().toFile();
    File sortedFile = workingFolder.resolve(sourceFile.getName() + "_sorted").toFile();

    GBIF_FILE_UTILS.sort(sourceFile, sortedFile, Charsets.UTF_8.toString(), idColumnIndex,
            dataFile.getDelimiterChar().toString(), null, "\n", dataFile.isHasHeaders() ? 1 : 0, null, caseSensitive);

    //FIXME doesn't support case sensitive for now
    String[] result = FileBashUtilities.findDuplicates(sortedFile.getAbsolutePath(), idColumnIndex + 1,
            dataFile.getDelimiterChar().toString());

    Arrays.stream(result).forEach(rec -> resultConsumer.accept(buildResult(rowType, rec)));
  }

  private static RecordEvaluationResult buildResult(Term rowType, String nonUniqueId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED,
            null, null));

    return new RecordEvaluationResult(rowType, null,  nonUniqueId, resultDetails, null);
  }

}
