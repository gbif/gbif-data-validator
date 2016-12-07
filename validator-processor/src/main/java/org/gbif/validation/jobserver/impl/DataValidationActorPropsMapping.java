package org.gbif.validation.jobserver.impl;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.ActorPropsMapping;
import org.gbif.validation.processor.ChecklistsValidatorActor;
import org.gbif.validation.processor.ParallelDataFileProcessorMaster;

import java.util.HashMap;
import java.util.Optional;

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
                                         NormalizerConfiguration normalizerConfiguration) {
    props =  Props.create(ParallelDataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
                                           workingDir, normalizerConfiguration);
  }

  /**
   * Gets the elements to build an actor for an specific data file.
   */
  @Override
  public Props getActorProps(DataFile dataFile) {
    return props;
  }
}
