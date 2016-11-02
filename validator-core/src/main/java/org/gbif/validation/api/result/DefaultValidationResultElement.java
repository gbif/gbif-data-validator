package org.gbif.validation.api.result;

import java.util.List;

/**
 * Minimal {@link ValidationResultElement} implementation.
 */
public class DefaultValidationResultElement implements ValidationResultElement {

  private final String fileName;
  private final List<ValidationIssue> issues;

  DefaultValidationResultElement(String fileName, List<ValidationIssue> issues) {
    this.fileName = fileName;
    this.issues = issues;
  }

  public String getFileName() {
    return fileName;
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

}
