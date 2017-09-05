package org.gbif.validation.api;

import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.IOException;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

/**
 * Contrary to {@link RecordEvaluator}, {@link RecordCollectionEvaluator} operates at a higher level representing more
 * than one record but, it also produces ({@link RecordEvaluationResult}) at a record level.
 */
public interface RecordCollectionEvaluator {

  /**
   * Evaluates a collection of record and produces (optionally since it may not produce any result)
   * {@link RecordEvaluationResult}. Since it may produce a large quantity of results and the caller may want to
   * limit and/or filter them, we use a {@link Consumer}.
   *
   * @param dwcDataFile
   * @param
   */
  void evaluate(@NotNull DwcDataFile dwcDataFile, @NotNull Consumer<RecordEvaluationResult> resultConsumer) throws IOException;
}
