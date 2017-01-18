package org.gbif.validation.jobserver.impl;

import org.gbif.validation.api.DataFile;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.processor.DataFileProcessorMaster;

import java.util.function.Supplier;

import akka.actor.Props;

/**
 * This class implements the factory properties to build Actor instances based on {@link DataFile}.
 */
public class ActorPropsSupplier implements Supplier<Props> {

  private final Props props;

  /**
   * Default constructor, the parameters received are used to build actor instances.
   */
  public ActorPropsSupplier(EvaluatorFactory evaluatorFactory, Integer fileSplitSize, String workingDir) {
    props =  Props.create(DataFileProcessorMaster.class, evaluatorFactory, fileSplitSize, workingDir);
  }

  /**
   * Gets the elements to build an actor for an specific data file.
   */
  @Override
  public Props get() {
    return props;
  }
}
