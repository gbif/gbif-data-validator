package org.gbif.validation.evaluator;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.nub.lookup.straight.IdLookupPassThru;
import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.TabularDataFile;
import org.gbif.validation.api.model.RecordEvaluationResult;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.util.OccurrenceToTermsHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.evaluator.InterpretationRemarkEvaluationTypeMapping.INTERPRETATION_REMARK_MAPPING;

/**
 * {@link RecordCollectionEvaluator} implementation to evaluate Checklist using ChecklistBank Normalizer.
 * Currently, no nub matching is done.
 * Not Thread-Safe.
 */
class ChecklistEvaluator implements RecordCollectionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(ChecklistEvaluator.class);
  private static final Predicate<InterpretationRemark> IS_MAPPED = INTERPRETATION_REMARK_MAPPING::containsKey;

  private final NormalizerConfiguration configuration;

  /**
   *
   * @param configuration
   * @param workingFolder where temporary results will be stored. The called is responsible to delete it.
   */
  public ChecklistEvaluator(NormalizerConfiguration configuration, Path workingFolder) {
    this.configuration = new NormalizerConfiguration();

    //use our own neo repository
    this.configuration.neo.neoRepository = workingFolder.resolve("neo").toFile();

    //copy other known configuration
    this.configuration.neo.batchSize = configuration.neo.batchSize;
    this.configuration.neo.mappedMemory = configuration.neo.mappedMemory;
    this.configuration.poolSize = configuration.poolSize;
  }


  /**
   * The NormalizerConfiguration instance is used to run a single Normalizer each time this method is executed.
   *
   * @throws IOException
   */
  @Override
  public void evaluate(DwcDataFile dwcDataFile, Consumer<RecordEvaluationResult> resultConsumer) throws IOException {

    List<TabularDataFile> taxonFile = dwcDataFile.getByRowType(DwcTerm.Taxon);
    Preconditions.checkState(!taxonFile.isEmpty(), "No Taxon TabularDataFile is defined");
    Preconditions.checkState(taxonFile.size() == 1, "More than 1 Taxon TabularDataFile is defined");

    //The generated a random dataset key, we only need it as a key
    UUID datasetKey = UUID.randomUUID();

    try (UsageDao dao = UsageDao.create(configuration.neo, datasetKey)) {
      Normalizer normalizer = Normalizer.create(datasetKey, dao, determinePathToUse(dwcDataFile).toFile(),
              new IdLookupPassThru(), configuration.neo.batchSize);
      normalizer.run(false);
      collectUsagesData(dao, resultConsumer);
    }
  }

  /**
   * If we are dealing with a Dwc-A the {@link Normalizer} expects the path to the archive folder, if we have a single
   * file, it expects the path ot this file.
   *
   * @param dwcDataFile
   *
   * @return
   */
  private Path determinePathToUse(DwcDataFile dwcDataFile) {
    return dwcDataFile.getDataFile().getFileFormat() == FileFormat.DWCA ?
            dwcDataFile.getDataFile().getFilePath() :
            dwcDataFile.getByRowType(DwcTerm.Taxon).get(0).getFilePath();
  }

  /**
   * Collect issues and graph data from the normalization result.
   */
  private void collectUsagesData(final UsageDao dao, final Consumer<RecordEvaluationResult> resultConsumer) {

    try (Transaction tx = dao.beginTx()) {
      // iterate over all node and collect their issues

      ResourceIterator<Node> it = dao.allNodes().iterator();
      while(it.hasNext()){
        Node node = it.next();
        NameUsage usage = dao.readUsage(node, false);
        resultConsumer.accept(toEvaluationResult(usage, dao.readVerbatim(node.getId())));
      }


        //usage.getIssues().stream().forEach( issue ->
        //        results.add(toEvaluationResult(usage, dao.readVerbatim(node.getId()))));
//      });
      //get the graph/tree
      //result.setGraph(getTree(dao, GraphFormat.TEXT));

      //we filter out results with no details. This can happen when the normalizer flag issue we are not interested in.
     // return results.stream().filter( rer -> rer.getDetails() != null && !rer.getDetails().isEmpty() );
     // return results.stream();
    }
  }

  /**
   * -- Visible For Testing --
   * Creates a RecordEvaluationResult from an NameUsage and VerbatimNameUsage.
   * Responsible to put the related data (e.g. field + current value) into the RecordEvaluationResult instance.
   * @param nameUsage
   * @param verbatimNameUsage
   * @return
   */
  protected RecordEvaluationResult toEvaluationResult(NameUsage nameUsage, VerbatimNameUsage verbatimNameUsage) {
    RecordEvaluationResult.Builder builder = RecordEvaluationResult.Builder.of(DwcTerm.Taxon, nameUsage.getTaxonID());
    builder.withInterpretedData(OccurrenceToTermsHelper.getTermsMap(nameUsage));
    builder.withVerbatimData(verbatimNameUsage.getFields());
    nameUsage.getIssues().stream().filter(IS_MAPPED).
            forEach(issue -> {
              Map<Term, String> relatedData = issue.getRelatedTerms()
                      .stream()
                      .filter(t -> verbatimNameUsage.getCoreField(t) != null)
                      .collect(Collectors.toMap(Function.identity(), verbatimNameUsage::getCoreField));
              builder.addInterpretationDetail(INTERPRETATION_REMARK_MAPPING.get(issue),
                      relatedData);
            });
    return builder.build();
  }

  /**
   * Gets the checklist tree.
   */
//  private static String getTree(UsageDao dao, GraphFormat format) {
//    // get tree
//    try (Writer writer = new StringWriter()) {
//      dao.printTree(writer, format);
//      return  writer.toString();
//    } catch (Exception ex) {
//      LOG.error("Error producing checklist graph", ex);
//      throw new RuntimeException(ex);
//    }
//  }

}
