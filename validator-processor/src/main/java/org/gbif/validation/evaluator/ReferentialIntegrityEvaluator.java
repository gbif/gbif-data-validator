package org.gbif.validation.evaluator;

import com.google.common.collect.ImmutableMap;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.util.FileBashUtilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the referential integrity of one Darwin Core
 * extension.
 * <br>
 * This checks for correct coreId and id fields between core and extension, and also that no coreId or id field is empty.
 */
class ReferentialIntegrityEvaluator implements RecordCollectionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(ReferentialIntegrityEvaluator.class);
  private static final int MAX_SAMPLE = 10;

  private final Term extensionRowType;

  ReferentialIntegrityEvaluator(Term extensionRowType) {
    Objects.requireNonNull(extensionRowType, "extensionRowType shall be provided");
    this.extensionRowType = extensionRowType;
  }

  /**
   * Run the evaluation on a {@link DataFile} representing the Dwc-A.
   *
   * @param dwcDataFile
   *
   * @return
   */
  @Override
  public void evaluate(DwcDataFile dwcDataFile, Consumer<RecordEvaluationResult> resultConsumer) throws IOException {

    TabularDataFile coreDf = dwcDataFile.getCore();
    TabularDataFile extDf = dwcDataFile.getByRowTypeKey(RowTypeKey.forExtension(extensionRowType));

    Preconditions.checkState(coreDf != null && coreDf.getRecordIdentifier().isPresent(),
            "DwcDataFile core shall have a record identifier");
    Preconditions.checkState(extDf != null && extDf.getRecordIdentifier().isPresent(),
            "DwcDataFile extension shall have a record identifier");

    String[] matchResult = FileBashUtilities.diffOnColumns(
            coreDf.getFilePath().toString(),
            extDf.getFilePath().toString(),
            coreDf.getRecordIdentifier().get().getIndex() + 1,
            extDf.getRecordIdentifier().get().getIndex() + 1,
            coreDf.getDelimiterChar().toString(),
            coreDf.isHasHeaders());

    Arrays.stream(matchResult).forEach(rec -> resultConsumer.accept(buildResult(extensionRowType, rec)));

    List<String[]> notEmptyCoreResult = FileBashUtilities.findEmpty(
      coreDf.getFilePath().toString(),
      coreDf.getRecordIdentifier().get().getIndex()+1,
      coreDf.getDelimiterChar().toString());

    notEmptyCoreResult.stream().forEach(rec -> resultConsumer.accept(buildResult2(ArchiveFile.DEFAULT_ID_TERM, rec)));

    List<String[]> notEmptyExtensionResult = FileBashUtilities.findEmpty(
      extDf.getFilePath().toString(),
      extDf.getRecordIdentifier().get().getIndex()+1,
      extDf.getDelimiterChar().toString());

    notEmptyExtensionResult.stream().forEach(rec -> resultConsumer.accept(buildResult2(extensionRowType, rec)));
  }

  private static RecordEvaluationResult buildResult(Term rowType, String unlinkedId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
            null, null));

    return new RecordEvaluationResult(rowType, null,  unlinkedId, resultDetails, null, null);
  }

  private static RecordEvaluationResult buildResult2(Term rowType, String[] emptyId) {
    List<RecordEvaluationResultDetails> resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
      ImmutableMap.of(rowType, emptyId[1])));

    return new RecordEvaluationResult(null, Long.valueOf(emptyId[0]), null, resultDetails, null, null);
  }
}
