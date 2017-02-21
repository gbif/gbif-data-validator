package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.source.DataFileFactory;
import org.gbif.validation.util.FileBashUtilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link RecordCollectionEvaluator} implementation to evaluate the referential integrity of one Darwin Core
 * extension.
 *
 */
public class ReferentialIntegrityEvaluator implements RecordCollectionEvaluator<DataFile> {

  private static final Logger LOG = LoggerFactory.getLogger(ReferentialIntegrityEvaluator.class);
  private static final int MAX_SAMPLE = 10;

  private final Term extensionRowType;

  ReferentialIntegrityEvaluator(Term extensionRowType) {
    this.extensionRowType = extensionRowType;
  }

  /**
   * Run the evaluation on a {@link DataFile} representing the Dwc-A.
   *
   * @param dataFile where the resource is located. The {@link DataFile} shall represent the entire Dwc Archive.
   *
   * @return
   */
  @Override
  public Optional<Stream<RecordEvaluationResult>> evaluate(DataFile dataFile) throws IOException {

    Archive archive = ArchiveFactory.openArchive(dataFile.getFilePath().toFile());

    ArchiveFile core = archive.getCore();
    ArchiveFile ext = archive.getExtension(extensionRowType);

    int coreIdIdx = core.getId().getIndex();
    int extCoreIdx = ext.getId().getIndex();

    List<TabularDataFile> dfList = DataFileFactory.prepareDataFile(dataFile);

    Map<Term, TabularDataFile> dfPerRowType = dfList.stream()
            .collect(Collectors.toMap(TabularDataFile::getRowType, Function.identity()));
    TabularDataFile coreDf = dfPerRowType.get(core.getRowType());
    TabularDataFile extDf = dfPerRowType.get(ext.getRowType());

    String[] result = FileBashUtilities.diffOnColumns(
            coreDf.getFilePath().toString(),
            extDf.getFilePath().toString(),
            coreIdIdx + 1,
            extCoreIdx + 1,
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
