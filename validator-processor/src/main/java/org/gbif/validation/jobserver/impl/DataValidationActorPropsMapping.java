package org.gbif.validation.jobserver.impl;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.evaluator.EvaluatorFactory;
import org.gbif.validation.jobserver.ActorPropsMapping;
import org.gbif.validation.processor.ChecklistsProcessorMaster;
import org.gbif.validation.processor.ParallelDataFileProcessorMaster;

import java.util.HashMap;
import java.util.Optional;

import akka.actor.Props;

/**
 * This class implements the factory properties to build Actor instances based on {@link DataFile}.
 */
public class DataValidationActorPropsMapping implements ActorPropsMapping<DataFile>{

  private final HashMap<Term, Props> mapping;

  /**
   * Default constructor, the parameters received are used to build actor instances.
   */
  public DataValidationActorPropsMapping(EvaluatorFactory evaluatorFactory, Integer fileSplitSize, String workingDir,
                                         NormalizerConfiguration normalizerConfiguration) {
    mapping = new HashMap<Term, Props> () {{
      put(DwcTerm.Occurrence, Props.create(ParallelDataFileProcessorMaster.class, evaluatorFactory, fileSplitSize,
                                           workingDir));
      put(DwcTerm.Taxon,Props.create(ChecklistsProcessorMaster.class, normalizerConfiguration));
     }};
  }

  /**
   * Gets the elements to build an actor for an specific data file.
   */
  @Override
  public Props getActorProps(DataFile dataFile) {
    if (Optional.ofNullable(dataFile.getRowType()).isPresent()) {
      return mapping.get(dataFile.getRowType());
    }
    //If the dataFile doesn't have a type defined, OCCURRENCE is assumed to be the default
    return mapping.get(DwcTerm.Occurrence);
  }
}
