package org.gbif.validation.api.result;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.checklistbank.cli.normalizer.NormalizerStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ChecklistValidationResult {

  private Map<NameUsageIssue,Set<NameUsage>> issues;

  private NormalizerStats stats;

  private String graph;

  private final int sampleSize;

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
      samples.add(nameUsage);
      issues.put(issue, samples);
    }
  }

  public NormalizerStats getStats() {
    return stats;
  }

  public void setStats(NormalizerStats stats) {
    this.stats = stats;
  }

  public String getGraph() {
    return graph;
  }

  public void setGraph(String graph) {
    this.graph = graph;
  }

  public int getSampleSize() {
    return sampleSize;
  }
}
