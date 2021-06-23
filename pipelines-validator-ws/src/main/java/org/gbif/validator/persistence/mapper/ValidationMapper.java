package org.gbif.validator.persistence.mapper;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.validator.api.Validation;

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.ibatis.annotations.Param;

public interface ValidationMapper {

  /**
   * This gets the instance in question. Note that this does return deleted items.
   *
   * @param key of the network entity to fetch
   * @return either the requested network entity or {@code null} if it couldn't be found
   */
  Validation get(@Param("key") UUID key);

  void create(Validation entity);

  void delete(@Param("key") UUID key);

  void update(Validation entity);

  List<Validation> list(@Nullable @Param("page") Pageable page, @Nullable @Param("username") String username);

  int count(@Nullable @Param("username") String username);

}
