package org.gbif.validation.jobserver;

import akka.actor.Props;

/**
 * Factory interface to get an instance of Props from a file format.
 * This interface is used to build instances of Akka actors that can be controlled by the JobMonitor actor.
 */
@FunctionalInterface
public interface ActorPropsMapping<T> {

  /**
   * Creates an actor Props for an specific fileFormat.
   */
  Props getActorProps(T input);

}
