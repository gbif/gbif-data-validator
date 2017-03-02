package org.gbif.validation.source;

/**
 *
 */
public class UnsupportedDataFileException extends Exception {

  UnsupportedDataFileException(String message){
    super(message);
  }

  UnsupportedDataFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
