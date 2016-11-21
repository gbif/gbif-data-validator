package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;

import java.util.HashMap;
import java.util.Map;

import akka.actor.Props;

public class DataValidationActorPropsMapping {


  private static final Map<FileFormat, Props> MAPPING = new HashMap() {{
                                                                          put(FileFormat.TABULAR,
                                                                              Props.create(ParallelDataFileProcessor.class));
                                                                      }};



  public Props getActorProps(FileFormat fileFormat) {
    return MAPPING.get(fileFormat);
  }
}
