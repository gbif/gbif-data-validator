package org.gbif.validation.collector;

import org.gbif.dwc.terms.Term;

import java.util.List;

/**
 * A {@link CollectorGroupProvider} is used to create new collector instances based on configurations.
 */
public class CollectorGroupProvider {

  private final Term rowType;
  private final List<Term> columns;

  /**
   *
   * @param rowType
   * @param columns
   */
  public CollectorGroupProvider(Term rowType, List<Term> columns) {
    this.rowType = rowType;
    this.columns = columns;
  }

  /**
   * Get a new {@link CollectorGroup} instance
   * @return
   */
  public CollectorGroup newCollectorGroup() {
    return new CollectorGroup(columns,
            CollectorFactory.createInterpretedTermsCountCollector(rowType, true));
  }

}
