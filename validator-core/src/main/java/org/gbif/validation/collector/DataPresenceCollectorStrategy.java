package org.gbif.validation.collector;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemark;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collect data related to the presence or not of an EvaluationType
 */
public class DataPresenceCollectorStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(DataPresenceCollectorStrategy.class);

  /**
   * Get the Set of (of interest?) InterpretationRemark based on a Set of OccurrenceIssue and a Set of term provided.
   * @param occurrenceIssue
   * @param providedTerm
   * @return
   */
  public static Set<InterpretationRemark> getInterpretationRemarks(Set<OccurrenceIssue> occurrenceIssue, Set<Term> providedTerm) {
    Set<InterpretationRemark> remarksSet = new HashSet<>();
    InterpretationRemark currInterpretationRemark;
    for(OccurrenceIssue currIssue : occurrenceIssue) {
      currInterpretationRemark = InterpretationRemarksDefinition.REMARKS_MAP.get(currIssue);

      if(currInterpretationRemark == null) {
        LOG.warn("OccurrenceIssue {} not found in InterpretationRemarksDefinition.REMARKS_MAP. This could be a bug.", currIssue);
      }
      else{
        // some remarks doesn't have any related terms
        if(currInterpretationRemark.getRelatedTerms() != null) {
          if( CollectionUtils.containsAny(currInterpretationRemark.getRelatedTerms(), providedTerm)) {
            remarksSet.add(currInterpretationRemark);
          }
        }
      }
    }
    return remarksSet;
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
