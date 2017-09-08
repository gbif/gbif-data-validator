package org.gbif.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.validation.api.vocabulary.DwcFileType;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Uniquely represents a rowType in the context of Darwin Core.
 * The main reason being that a core and an extension can possibly have the same rowType.
 */
public class RowTypeKey {

  /**
   * Special singleton instance for core without rowType.
   */
  public static RowTypeKey CORE_WITHOUT_ROW_TYPE = new RowTypeKey(null, DwcFileType.CORE);
  public static RowTypeKey EXT_WITHOUT_ROW_TYPE = new RowTypeKey(null, DwcFileType.EXTENSION);

  private final DwcFileType dwcFileType;
  private final Term rowType;

  /**
   * Build a new {@link RowTypeKey}.
   * The difference with {@link #of(Term, DwcFileType)} is simply that this function will not throw
   * exception if rowType is not provided but instead will provide a singleton instance of WITHOUT_ROW_TYPE.
   *
   * @param rowType
   * @param dwcFileType
   * @return
   */
  public static RowTypeKey get(@Nullable Term rowType, @NotNull DwcFileType dwcFileType) {
    if(rowType == null){
      return withoutRowType(dwcFileType);
    }
    return of(rowType, dwcFileType);
  }

  /**
   * Build a new {@link RowTypeKey}.
   * @param rowType
   * @param dwcFileType
   * @return
   */
  public static RowTypeKey of(Term rowType, DwcFileType dwcFileType) {
    java.util.Objects.requireNonNull(dwcFileType, "dwcFileType shall be provided");
    java.util.Objects.requireNonNull(rowType, "rowType shall be provided");
    Preconditions.checkArgument(dwcFileType.isDataBased(), "DwcFileType shall be dataBased");
    return new RowTypeKey(rowType, dwcFileType);
  }

  public static RowTypeKey forCore(Term rowType) {
    return of(rowType, DwcFileType.CORE);
  }

  public static RowTypeKey forExtension(Term rowType) {
    return of(rowType, DwcFileType.EXTENSION);
  }

  /**
   * Only used to express a {@link RowTypeKey} when a row type can NOT be found.
   * @param dwcFileType
   * @return
   */
  public static RowTypeKey withoutRowType(DwcFileType dwcFileType) {
    if(DwcFileType.CORE == dwcFileType) {
      return CORE_WITHOUT_ROW_TYPE;
    }
    if(DwcFileType.EXTENSION == dwcFileType) {
      return EXT_WITHOUT_ROW_TYPE;
    }
    throw new IllegalArgumentException("dwcFileType can only be CORE or EXTENSION");
  }

  private RowTypeKey(Term rowType, DwcFileType dwcFileType) {
    this.rowType = rowType;
    this.dwcFileType = dwcFileType;
  }

  public DwcFileType getDwcFileType() {
    return dwcFileType;
  }

  public Term getRowType() {
    return rowType;
  }

  /**
   * Return a string in the form "core_Taxon"
   * @return
   */
  public String name() {
    return dwcFileType.name().toLowerCase() + "_" + (rowType != null ? rowType.simpleName() : "WITHOUT_ROW_TYPE");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof RowTypeKey) {
      RowTypeKey that = (RowTypeKey) obj;
      return Objects.equal(dwcFileType, that.dwcFileType)
              && Objects.equal(this.rowType, that.rowType);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(dwcFileType, rowType);
  }
}
