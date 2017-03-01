package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.util.FileBashUtilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the referential integrity of one Darwin Core
 * extension.
 *
 */
public class ReferentialIntegrityEvaluator implements RecordCollectionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(ReferentialIntegrityEvaluator.class);
  private static final int MAX_SAMPLE = 10;

  private final Term extensionRowType;

  ReferentialIntegrityEvaluator(Term extensionRowType) {
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
  public Optional<Stream<RecordEvaluationResult>> evaluate(DwcDataFile dwcDataFile) throws IOException {

    TabularDataFile coreDf = dwcDataFile.getCore();
    TabularDataFile extDf = dwcDataFile.getByRowType(extensionRowType);

    Preconditions.checkState(coreDf != null && coreDf.getRecordIdentifier().isPresent(),
            "DwcDataFile core shall have a record identifier");
    Preconditions.checkState(extDf != null && extDf.getRecordIdentifier().isPresent(),
            "DwcDataFile extension shall have a record identifier");

    String[] result = FileBashUtilities.diffOnColumns(
            coreDf.getFilePath().toString(),
            extDf.getFilePath().toString(),
            coreDf.getRecordIdentifier().get().getIndex() + 1,
            extDf.getRecordIdentifier().get().getIndex() + 1,
            coreDf.getDelimiterChar().toString(),
            coreDf.isHasHeaders());

    return Optional.of(Arrays.stream(result).map(rec -> buildResult(extensionRowType, rec)));
  }

  private static RecordEvaluationResult buildResult(Term rowType, String unlinkedId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
            null, null));

    return new RecordEvaluationResult(rowType, null,  unlinkedId, resultDetails,null);
  }

}
