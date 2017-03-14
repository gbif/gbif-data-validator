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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.validation.evaluator.InterpretationRemarkEvaluationTypeMapping.INTERPRETATION_REMARK_MAPPING;


/**
 * {@link RecordCollectionEvaluator} implementation to evaluate Checklist using ChecklistBank Normalizer.
 * Currently, no nub matching is done.
 * Not Thread-Safe.
 */
public class ChecklistEvaluator implements RecordCollectionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(ChecklistEvaluator.class);
  private static final Predicate<InterpretationRemark> IS_MAPPED = issue -> INTERPRETATION_REMARK_MAPPING.containsKey(issue);

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
   * @return
   * @throws IOException
   */
  @Override
  public Optional<Stream<RecordEvaluationResult>> evaluate(DwcDataFile dwcDataFile) throws IOException {

    TabularDataFile taxonFile = dwcDataFile.getByRowType(DwcTerm.Taxon);
    Preconditions.checkNotNull(taxonFile, "No Taxon TabularDataFile is defined");

    //The generated a random dataset key, we only need it as a key
    UUID datasetKey = UUID.randomUUID();
    try {
      //Run normalizer with no NubLookup
      Normalizer normalizer = Normalizer.create(configuration, datasetKey, taxonFile.getFilePath().toFile(),
              new MetricRegistry(), Maps.newHashMap(), new IdLookupPassThru());
      normalizer.run();
     // result.setStats(normalizer.getStats());

      return Optional.of(collectUsagesData(datasetKey));
    } catch (Exception ex) {
      LOG.error("Error running checklist normalizer", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Collect issues and graph data from the normalization result.
   */
  private Stream<RecordEvaluationResult> collectUsagesData(UUID datasetKey) {
    List<RecordEvaluationResult> results = new ArrayList<>();
    //FIX ME, should be READ_ONLY but it throws:
    /**
     Caused by: java.lang.UnsupportedOperationException: Can't mark read only index.
     at org.neo4j.kernel.api.impl.schema.ReadOnlyDatabaseSchemaIndex.markAsOnline(ReadOnlyDatabaseSchemaIndex.java:93)
     at org.neo4j.kernel.api.impl.schema.LuceneIndexAccessor.force(LuceneIndexAccessor.java:77)
     at org.neo4j.kernel.impl.api.index.OnlineIndexProxy.force(OnlineIndexProxy.java:133)
     at org.neo4j.kernel.impl.api.index.AbstractDelegatingIndexProxy.force(AbstractDelegatingIndexProxy.java:82)
     at org.neo4j.kernel.impl.api.index.ContractCheckingIndexProxy.force(ContractCheckingIndexProxy.java:125)
     at org.neo4j.kernel.impl.api.index.IndexingService.forceAll(IndexingService.java:697)
     at org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine.flushAndForce(RecordStorageEngine.java:473)
     at org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl.doCheckPoint(CheckPointerImpl.java:203)
     at org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl.forceCheckPoint(CheckPointerImpl.java:114)
     at org.neo4j.kernel.NeoStoreDataSource$5.shutdown(NeoStoreDataSource.java:917)
     at org.neo4j.kernel.lifecycle.LifeSupport$LifecycleInstance.shutdown(LifeSupport.java:488)
     */
    UsageDao dao = UsageDao.persistentDao(configuration.neo, datasetKey, false, null, false);
    try (Transaction tx = dao.beginTx()) {
      // iterate over all node and collect their issues
      StreamSupport.stream(dao.allNodes().spliterator(),false).forEach(node -> {
        NameUsage usage = dao.readUsage(node, false);
        usage.getIssues().stream().forEach( issue ->
                results.add(toEvaluationResult(usage, dao.readVerbatim(node.getId()))));
      });
      //get the graph/tree
      //result.setGraph(getTree(dao, GraphFormat.TEXT));

      //we filter out results with no details. This can happen when the normalizer flag issue we are not interested in.
      return results.stream().filter( rer -> rer.getDetails() != null && !rer.getDetails().isEmpty() );
    } finally {
      if (dao != null) {
        dao.close();
      }
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

   // Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();
  //  builder.withInterpretedData(OccurrenceToTermsHelper.getTermsMap(result.getUpdated()));

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
