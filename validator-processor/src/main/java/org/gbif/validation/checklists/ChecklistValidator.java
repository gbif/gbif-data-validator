package org.gbif.validation.checklists;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.checklistbank.cli.model.GraphFormat;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.result.ChecklistValidationResult;
import org.gbif.validation.api.DataFile;

import java.io.File;
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

  private final NormalizerConfiguration configuration;

  /**
   * Default constructor: requires a NormalizerConfiguration object.
   */
  public ChecklistValidator(NormalizerConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Validates a checklist file. T
   * The NormalizerConfiguration instance is used to run a single Normalizer each time this method is executed.
   */
  public ChecklistValidationResult validate(DataFile dataFile) {
    //The dataset key is obtained from the temporary directory name which is assumed to be an UUID
    UUID datasetKey = UUID.fromString(dataFile.getFilePath().getParent().getFileName().toString());
    try {
      ChecklistValidationResult result = new ChecklistValidationResult(SAMPLE_SIZE);
      //Run normalizer
      Normalizer normalizer = Normalizer.create(configuration, datasetKey);
      normalizer.run();
      result.setStats(normalizer.getStats());
      collectUsagesData(datasetKey, result);
      return result;
    } catch(Exception ex) {
      LOG.error("Error running checklist normalizer", ex);
      throw new RuntimeException(ex);
    } finally {
      removeTempDirs(datasetKey);
    }
  }

  /**
   * Collect issues and graph data from the normalization result.
   */
  private void collectUsagesData(UUID datasetKey, ChecklistValidationResult result) {
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
  }

  /**
   * Gets the checklist tree.
   */
  private static String getTree(UsageDao dao, GraphFormat format) {
    // get tree
    try (Writer writer = new StringWriter()) {
      dao.printTree(writer, format);
      return  writer.toString();
    } catch (Exception ex) {
      LOG.error("Error producing checklist graph",ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Remove temporary directories created to validate the data file.
   */
  private void removeTempDirs(UUID datasetKey) {
    deleteIfExists(configuration.archiveDir(datasetKey));
    deleteIfExists(configuration.neo.kvp(datasetKey));
    deleteIfExists(configuration.neo.neoDir(datasetKey));
  }

  /**
   * Deletes a file or directory recursively if it exists.
   */
  private static void deleteIfExists(File file) {
    if(file.exists()) {
      if(file.isDirectory()) {
        FileUtils.deleteDirectoryRecursively(file);
      } else {
        file.delete();
      }
    }
  }

}
