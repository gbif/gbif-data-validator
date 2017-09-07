package org.gbif.validation.conf;

import org.gbif.ws.util.ExtraMediaTypes;

import java.util.List;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.ImmutableList;

/**
 * Contains MediaType supprted by the validator.
 */
public class SupportedMediaTypes {

  private SupportedMediaTypes() {}

  public static final List<String> ZIP_CONTENT_TYPE = ImmutableList.of(
          org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString(),
          ExtraMediaTypes.APPLICATION_GZIP);

  public static final List<String> TABULAR_CONTENT_TYPES = ImmutableList.of(MediaType.TEXT_PLAIN,
          ExtraMediaTypes.TEXT_CSV,
          ExtraMediaTypes.TEXT_TSV);

  public static final List<String> SPREADSHEET_CONTENT_TYPES = ImmutableList.of(
          ExtraMediaTypes.APPLICATION_EXCEL,
          ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET,
          ExtraMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET);

}
