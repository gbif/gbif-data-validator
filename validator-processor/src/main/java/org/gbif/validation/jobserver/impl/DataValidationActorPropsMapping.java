package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.ActorPropsMapping;
import org.gbif.validation.tabular.parallel.ParallelDataFileProcessorMaster;

import java.util.HashMap;
import java.util.Map;

import akka.actor.Props;

/**
 * This class implements the factory properties to build Actor instances,
 */
public class DataValidationActorPropsMapping  implements ActorPropsMapping<DataFile>{


  //Defines a mapping between file formats and the type of actor that handles it
  private final Map<FileFormat, Props> mapping;

  /**
   * Default constructor, the parameters received are used to build actor instances.
   */
  public DataValidationActorPropsMapping(EvaluatorFactory evaluatorFactory, Integer fileSplitSize, String workingDir) {
    mapping = new HashMap<FileFormat, Props> () {{
        put(FileFormat.TABULAR,
                    Props.create(ParallelDataFileProcessorMaster.class, evaluatorFactory, fileSplitSize, workingDir));
      }};
  }

  /**
   * Gets the elements to build an actor for an specific data file.
   */
  public Props getActorProps(DataFile dataFile) {
    return mapping.get(dataFile.getFileFormat());
  }
}
