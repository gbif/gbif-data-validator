package org.gbif.validation.api;

import org.gbif.validation.api.model.RecordEvaluationResult;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;

/**
 * Responsible to evaluate a resource and check the integrity of its records.
 * Operating at {@link DataFile} level but returns results ({@link RecordEvaluationResult}) on a record level.
 */
public interface ResourceRecordIntegrityEvaluator {

  /**
   * Evaluates a {@link DataFile} and produce (optionally since it may not produce any result) a {@link Stream} of
   * {@link RecordEvaluationResult}. {@link Stream} is used since it may produce a large quantity of results and the
   * caller may want to limit and/or filter them.
   *
   * @param dataFile
   *
   * @return optionally, a {@link Stream} of {@link RecordEvaluationResult}.
   */
  Optional<Stream<RecordEvaluationResult>> evaluate(@NotNull DataFile dataFile) throws IOException;
}
