package org.gbif.validation.util;


import java.io.IOException;

/**
 * Same as {@link java.util.function.Function} but {@link #apply(Object)} throws {@link IOException}
 */
@FunctionalInterface
public interface IOFunction <T, R> {
  R apply(T d) throws IOException;
}
