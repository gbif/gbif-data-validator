package org.gbif.validation.api.result;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.checklistbank.cli.normalizer.NormalizerStats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class ChecklistValidationResult implements Serializable {

  private Map<NameUsageIssue,Set<NameUsage>> issues;

  private NormalizerStats stats;

  private String graph;

  @JsonIgnore
  private final int sampleSize;

  /**
   *
   * @param sampleSize maximum number of samples to store/display
   */
  public ChecklistValidationResult(int sampleSize) {
    issues = new HashMap<>();
    this.sampleSize = sampleSize;
  }

  public Map<NameUsageIssue, Set<NameUsage>> getIssues() {
    return issues;
  }

  public void setIssues(Map<NameUsageIssue, Set<NameUsage>> issues) {
    this.issues = issues;
  }

  public void addIssue(NameUsageIssue issue, NameUsage nameUsage) {
    Optional<Set<NameUsage>> currentSamples = Optional.ofNullable(issues.get(issue));
    Set<NameUsage> samples = currentSamples.orElse(new HashSet<>());
    if (samples.size() < sampleSize) {
      //Issues are not stored since the response is grouped by issues already
      nameUsage.getIssues().clear();
      samples.add(nameUsage);
      issues.put(issue, samples);
    }
  }

  /**
   * Statistics collected by the checklist normalizer.
   */
  public NormalizerStats getStats() {
    return stats;
  }

  public void setStats(NormalizerStats stats) {
    this.stats = stats;
  }

  /**
   * Hierarchical representation of the validated checklist data.
   */
  public String getGraph() {
    return graph;
  }

  public void setGraph(String graph) {
    this.graph = graph;
  }

  /**
   * Maximum number of samples to be stored for each NameUsageIssue.
   */
  public int getSampleSize() {
    return sampleSize;
  }
}
