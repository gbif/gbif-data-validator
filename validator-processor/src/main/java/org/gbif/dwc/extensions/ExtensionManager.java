package org.gbif.dwc.extensions;

import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

/**
 * Main manager for {@link Extension}.
 *
 * Moved from https://github.com/gbif/dwca-validator3
 */
public interface ExtensionManager {

  /**
   * Get an {@link Extension} for a specific rowType.
   * @param rowType
   * @return the {@link Extension} or {@code null} is not found
   */
  Extension get(Term rowType);


  /**
   * List all the {@link Extension} contained in the manager.
   * @return
   */
  List<Extension> list();


  /**
   * Get all the {@link Extension} contained in the manager organized by {@link Term}.
   * @return
   */
  Map<Term, Extension> map();

  /**
   * Search for a keyword on subject of the {@link Extension} contained in this manager.
   * @param keyword
   * @return
   */
  List<Extension> search(String keyword);


}
