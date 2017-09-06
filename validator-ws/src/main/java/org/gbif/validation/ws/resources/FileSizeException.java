package org.gbif.validation.ws.resources;

import java.io.IOException;

/**
 * Exception to expose file size exception as a single exception.
 */
public class FileSizeException extends IOException {
  FileSizeException(String pMsg) {
    super(pMsg);
  }

  FileSizeException(Exception ex) {
    super(ex);
  }
}