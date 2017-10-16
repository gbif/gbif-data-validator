package org.gbif.validation.processor;

/**
 * Simple message to indicate we created all actors required.
 */
class FinishedInit {
  static FinishedInit INSTANCE = new FinishedInit();
  private FinishedInit(){}
}
