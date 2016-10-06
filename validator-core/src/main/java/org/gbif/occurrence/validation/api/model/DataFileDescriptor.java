package org.gbif.occurrence.validation.api.model;

import java.nio.charset.Charset;

public class DataFileDescriptor {

  private String fileName;

  private FileFormat format;

  private Charset encoding;

  private String linesTerminatedBy="\r\n";

  private Character fieldsTerminatedBy= ',';

  private Character fieldsEnclosedBy='"';

  private String dateFormat;

  private Character decimalSeparator = ',';

  private boolean hasHeaders;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(FileFormat format) {
    this.format = format;
  }

  public Charset getEncoding() {
    return encoding;
  }

  public void setEncoding(Charset encoding) {
    this.encoding = encoding;
  }

  public String getLinesTerminatedBy() {
    return linesTerminatedBy;
  }

  public void setLinesTerminatedBy(String linesTerminatedBy) {
    this.linesTerminatedBy = linesTerminatedBy;
  }

  public Character getFieldsTerminatedBy() {
    return fieldsTerminatedBy;
  }

  public void setFieldsTerminatedBy(Character fieldsTerminatedBy) {
    this.fieldsTerminatedBy = fieldsTerminatedBy;
  }

  public Character getFieldsEnclosedBy() {
    return fieldsEnclosedBy;
  }

  public void setFieldsEnclosedBy(Character fieldsEnclosedBy) {
    this.fieldsEnclosedBy = fieldsEnclosedBy;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public Character getDecimalSeparator() {
    return decimalSeparator;
  }

  public void setDecimalSeparator(Character decimalSeparator) {
    this.decimalSeparator = decimalSeparator;
  }

  public boolean isHasHeaders() {
    return hasHeaders;
  }

  public void setHasHeaders(boolean hasHeaders) {
    this.hasHeaders = hasHeaders;
  }
}
