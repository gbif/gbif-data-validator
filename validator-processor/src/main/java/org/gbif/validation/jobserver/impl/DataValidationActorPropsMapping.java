package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.ActorPropsMapping;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessorMaster;

import java.util.HashMap;
import java.util.Map;

import akka.actor.Props;

public class DataValidationActorPropsMapping  implements ActorPropsMapping<DataFile>{


  private final Map<FileFormat, Props> mapping;


  public DataValidationActorPropsMapping(EvaluatorFactory evaluatorFactory, Integer fileSplitSize) {
    mapping = new HashMap<FileFormat, Props> () {{
        put(FileFormat.TABULAR,
                    Props.create(ParallelDataFileProcessorMaster.class, evaluatorFactory, fileSplitSize));
      }};
  }

  public Props getActorProps(DataFile dataFile) {
    return mapping.get(dataFile.getFileFormat());
  }
}
