package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.model.RecordEvaluationResultDetails;
import org.gbif.validation.collector.DwcExtensionIntegrityValidation;
import org.gbif.validation.source.RecordSourceFactory;

import java.io.IOException;
import java.util.ArrayList;
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
   * @param dataFile where the resource is located. The {@link DataFile} shall represent the entire Dwc Archive.
   * @return
   */
  @Override
  public Optional<Stream<RecordEvaluationResult>> evaluate(DataFile dataFile) throws IOException {

      Archive archive = ArchiveFactory.openArchive(dataFile.getFilePath().toFile());

      ArchiveFile core = archive.getCore();
      ArchiveFile ext = archive.getExtension(extensionRowType);

      int coreIdIdx = core.getId().getIndex();
      int extCoreIdx = ext.getId().getIndex();

      List<DataFile> dfList = RecordSourceFactory.prepareSource(dataFile);

      Map<Term, DataFile> dfPerRowType = dfList.stream()
              .collect(Collectors.toMap(DataFile::getRowType, Function.identity()));

      DataFile coreDf = dfPerRowType.get(core.getRowType());
      DataFile extDf = dfPerRowType.get(ext.getRowType());

      List<String> unlinkedId = DwcExtensionIntegrityValidation.collectUnlinkedExtensions(coreDf, coreIdIdx, extDf,
              extCoreIdx, MAX_SAMPLE);

      if (unlinkedId.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(unlinkedId.stream().map(rec -> buildResult(extensionRowType, rec)));
  }

//  private static ValidationResultElement buildResult(DataFile dataFile, List<String> unlinkedId){
//    List<ValidationResultDetails> resultDetails = new ArrayList<>();
//    unlinkedId.forEach(id ->
//            resultDetails.add(ValidationResultDetails.recordIdOnly(id)));
//
//    Map<EvaluationType, Long> issueCounter = new EnumMap<>(EvaluationType.class);
//    Map<EvaluationType, List<ValidationResultDetails>> issueSampling = new EnumMap<>(EvaluationType.class);
//    issueCounter.put(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION, (long) unlinkedId.size());
//    issueSampling.put(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION, resultDetails);
//
//    return new ValidationResultElement(dataFile.getSourceFileName(), (long) dataFile.getNumOfLines(),
//            dataFile.getRowType(), issueCounter, issueSampling, null, null);
//  }

  private static RecordEvaluationResult buildResult(Term rowType, String unlinkedId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
            null, null));

    return new RecordEvaluationResult(rowType, null,  unlinkedId, resultDetails,null);
  }

  private static RecordEvaluationResult buildExceptionResult(Term rowType, String unlinkedId){
    List<RecordEvaluationResultDetails>resultDetails = new ArrayList<>(1);
    resultDetails.add(new RecordEvaluationResultDetails(EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
            null, null));

    return new RecordEvaluationResult(rowType, null,  unlinkedId, resultDetails,null);
  }

}
