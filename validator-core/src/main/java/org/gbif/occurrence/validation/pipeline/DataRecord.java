package org.gbif.occurrence.validation.pipeline;

import org.gbif.dwc.terms.Term;

public class DataRecord<T> {

  private Long line;

  private T data;

  private Term[] terms;

  public Long getLine() {
    return line;
  }

  public void setLine(Long line) {
    this.line = line;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }

  public Term[] getTerms() {
    return terms;
  }

  public void setTerms(Term[] terms) {
    this.terms = terms;
  }
}
