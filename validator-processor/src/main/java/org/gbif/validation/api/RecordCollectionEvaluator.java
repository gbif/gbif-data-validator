package org.gbif.validation.api;

import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;

/**
 * Contrary to {@link RecordEvaluator}, {@link RecordCollectionEvaluator} operates at a higher level representing more
 * than one record but, it also produces ({@link RecordEvaluationResult}) at a record level.
 */
public interface RecordCollectionEvaluator {

  /**
   * Evaluates a record collection of type <T> and produces (optionally since it may not produce any result) a {@link
   * Stream} of
   * {@link RecordEvaluationResult}. {@link Stream} is used since it may produce a large quantity of results and the
   * caller may want to limit and/or filter them.
   *
   * @param dwcDataFile
   *
   * @return optionally, a {@link Stream} of {@link RecordEvaluationResult}.
   */
  Optional<Stream<RecordEvaluationResult>> evaluate(@NotNull DwcDataFile dwcDataFile) throws IOException;
}
