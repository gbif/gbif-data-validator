package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.checklists.ChecklistValidator;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.ActorPropsMapping;
import org.gbif.validation.processor.ParallelDataFileProcessorMaster;

import akka.actor.Props;

/**
 * This class implements the factory properties to build Actor instances based on {@link DataFile}.
 */
public class DataValidationActorPropsMapping implements ActorPropsMapping<DataFile>{

  private final Props props;

  /**
   * Default constructor, the parameters received are used to build actor instances.
   */
  public DataValidationActorPropsMapping(EvaluatorFactory evaluatorFactory, Integer fileSplitSize, String workingDir,
                                         ChecklistValidator checklistValidator) {
    props =  Props.create(ParallelDataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
                                           workingDir, checklistValidator);
  }

  /**
   * Gets the elements to build an actor for an specific data file.
   */
  @Override
  public Props getActorProps(DataFile dataFile) {
    return props;
  }
}
