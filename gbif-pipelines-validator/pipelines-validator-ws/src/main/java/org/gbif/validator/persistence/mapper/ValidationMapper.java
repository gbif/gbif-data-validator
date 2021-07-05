package org.gbif.validator.persistence.mapper;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.validator.api.Validation;

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper for CRUD and list database operations.
 */
public interface ValidationMapper {

  /**
   * Gets a Validation by its key/identifier.
   * @param key validation identifier
   * @return
   */
  Validation get(@Param("key") UUID key);

  /**
   * Creates/persists a validation.
   * @param validation to be created
   */
  void create(Validation validation);

  /**
   * Logical deletion of a validation.
   * @param key validation identifier
   */
  void delete(@Param("key") UUID key);

  /**
   * Updates the allowed information of a validation, changes the modified date to now.
   * @param validation to be updated
   */
  void update(Validation validation);

  /**
   * Paginates thru validations, optionally filtered by username.
   * @param page paging parameters
   * @param username filter
   * @return a list of validation
   */
  List<Validation> list(@Nullable @Param("page") Pageable page, @Nullable @Param("username") String username);

  /**
   * Counts the number of validations of a optional user parameter.
   * @param username filter
   * @return number of validations
   */
  int count(@Nullable @Param("username") String username);

}
