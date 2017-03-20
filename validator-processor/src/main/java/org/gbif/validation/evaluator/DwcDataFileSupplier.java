package org.gbif.validation.evaluator;

import org.gbif.validation.api.DwcDataFile;
import org.gbif.validation.source.UnsupportedDataFileException;

import java.io.IOException;

/**
 * The main purpose of this class is to allow to throw exceptions on the get.
 */
@FunctionalInterface
public interface DwcDataFileSupplier {
  DwcDataFile get() throws IOException, UnsupportedDataFileException;
}
