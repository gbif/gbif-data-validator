package org.gbif.validator.api;

import java.util.Date;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Validation {

  public enum Status {
    SUBMITTED, RUNNING, FINISHED;
  }

  private UUID key;
  private Date created;
  private Date modified;
  private String username;
  private String result;
  private String file;
  private FileFormat fileFormat;
  private Status status;

}
