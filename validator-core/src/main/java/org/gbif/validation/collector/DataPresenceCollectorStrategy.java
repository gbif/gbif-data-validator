package org.gbif.validation.collector;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemark;
import static org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition.REMARKS_MAP;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;


/**
 * Collect data related to the presence or not of an EvaluationType
 */
public class DataPresenceCollectorStrategy {

  /**
   * Get the Set of (of interest?) InterpretationRemark based on a Set of OccurrenceIssue and a Set of term provided.
   * @param occurrenceIssues
   * @param providedTerm
   * @return
   */
  public static Set<InterpretationRemark> getInterpretationRemarks(Set<OccurrenceIssue> occurrenceIssues, Set<Term> providedTerm) {
    return REMARKS_MAP.entrySet().stream().filter(issueRemark ->
                                                    occurrenceIssues.contains(issueRemark.getKey())
                                                    && issueRemark.getValue().getRelatedTerms() != null
                                                    && issueRemark.getValue().getRelatedTerms().stream().anyMatch(providedTerm::contains))
                                           .map(Map.Entry::getValue).collect(Collectors.toSet());
  }

  public void getIssueWithData(Set<InterpretationRemark> remarks, Map<Term, Integer> columns, String[] dataLine) {
    List<OccurrenceIssue> occurrenceIssueWithData = new ArrayList<>();
    for(InterpretationRemark a : remarks) {
      for (Term t : a.getRelatedTerms()) {
        if(columns.containsKey(t) && StringUtils.isNotBlank(dataLine[columns.get(t)])) {
          occurrenceIssueWithData.add(a.getType());
          break;
        }
      }
    }
  }

}
