package org.gbif.validation;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.checklistbank.cli.model.GraphFormat;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.validation.api.ChecklistValidationResult;
import org.gbif.validation.api.DataFile;

import java.io.StringWriter;
import java.io.Writer;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates validation of checklists.
 * Internally uses the the checklist Normalizer and UsageDao (Neo4j) to collect results.
 */
public class ChecklistValidator {

  private static final int SAMPLE_SIZE = 10;

  private static final Logger LOG = LoggerFactory.getLogger(ChecklistValidator.class);

  public static ChecklistValidationResult validate(DataFile dataFile, NormalizerConfiguration configuration) {
    //The dataset key is obtained from the temporary directory name which is assumed to be an UUID
    UUID datasetKey = UUID.fromString(dataFile.getFilePath().getParent().getFileName().toString());
    ChecklistValidationResult result = new ChecklistValidationResult(SAMPLE_SIZE);
    //Run normalizer
    Normalizer normalizer = Normalizer.create(configuration, datasetKey);
    normalizer.run();
    result.setStats(normalizer.getStats());

    UsageDao dao = UsageDao.persistentDao(configuration.neo, datasetKey, true, null, false);
    try (Transaction tx = dao.beginTx()) {
      // iterate over all node and collect their issues
      StreamSupport.stream(dao.allNodes().spliterator(),false).forEach(node -> {
        NameUsage usage = dao.readUsage(node, false);
        usage.getIssues().stream().forEach( issue -> result.addIssue(issue, usage));
      });
      //get the graph/tree
      result.setGraph(getTree(dao,GraphFormat.TEXT));
    } finally {
      if(dao != null) {
        dao.close();
      }
    }
    return result;
  }

  /**
   * Gets the checklist tree.
   */
  private static String getTree(UsageDao dao, GraphFormat format) {
    // get tree
    try (Writer writer = new StringWriter()) {
      dao.printTree(writer, format);
      return  writer.toString();
    } catch (Exception e) {
      LOG.error("Error producing checklist graph",e);
      throw new RuntimeException(e);
    }
  }
}
