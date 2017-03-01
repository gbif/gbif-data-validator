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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the uniqueness of the records identifier
 * in a {@link TabularDataFile}
 */
public class UniquenessEvaluator implements RecordCollectionEvaluator {

  org.gbif.utils.file.FileUtils GBIF_FILE_UTILS = new org.gbif.utils.file.FileUtils();

  private final Term rowType;
  private boolean caseSensitive;

  /**
   *
   * @param rowType Term used as identifier for the dataFile (start at 1)
   */
  public UniquenessEvaluator(Term rowType, boolean caseSensitive) {
    this.rowType = rowType;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Optional<Stream<RecordEvaluationResult>> evaluate(@NotNull DwcDataFile dwcDataFile) throws IOException {

    TabularDataFile dataFile = dwcDataFile.getByRowType(rowType);
    Preconditions.checkState(dataFile != null && dataFile.getRecordIdentifier().isPresent(),
            "DwcDataFile {} shall have a record identifier", rowType);
    int idColumnIndex = dataFile.getRecordIdentifier().get().getIndex();

    File sourceFile = dataFile.getFilePath().toFile();
    File sortedFile = dataFile.getFilePath().getParent().resolve(sourceFile.getName() + "_sorted").toFile();

    GBIF_FILE_UTILS.sort(sourceFile, sortedFile, Charsets.UTF_8.toString(), idColumnIndex,
            dataFile.getDelimiterChar().toString(), null, "\n", dataFile.isHasHeaders() ? 1 : 0, null, caseSensitive);

    //FIXME doesn't support case sensitive for now
    String[] result = FileBashUtilities.findDuplicates(sortedFile.getAbsolutePath(), idColumnIndex + 1,
            dataFile.getDelimiterChar().toString());

    return Optional.of(Arrays.stream(result).map(rec -> buildResult(rowType, rec)));
  }

  private static RecordEvaluationResult buildResult(Term rowType, String nonUniqueId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED,
            null, null));

    return new RecordEvaluationResult(rowType, null,  nonUniqueId, resultDetails, null);
  }

}
