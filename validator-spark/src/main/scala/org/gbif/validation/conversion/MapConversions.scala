package org.gbif.validation.conversion

import java.util.HashMap

import org.apache.spark.sql.Row
import org.gbif.dwc.terms.Term

import scala.collection.immutable.List

/**
  * Utility to convert between java and scala maps.
  */
object MapConversions {

  implicit class LongValueMapConversion[K](val m: Map[K, Long])
  {

    /**
      * Converts a scala util.Map into a Java mutable map.
      * This conversion is needed when serializing maps in Kryo.
      */
    def toMutableJavaMap: java.util.Map[K, java.lang.Long]  =
    {
      val newMap: HashMap[K, java.lang.Long] = new HashMap(m.size)
      m.foreach({ case (key, cnt) => {
        newMap.put(key, long2Long(cnt))
      }})
      newMap
    }

    /**
      * Adds values of m2 to m1.
      */
    def accumulate(m2: Map[K, Long]): Map[K, Long] = {
      m ++ (for ((k, v) <- m2) yield (k -> (v + m.getOrElse(k, 0L))))
    }
  }

  implicit class TermsConversion(val terms: List[Term]) {

    /**
      * Converts a list of terms and values into a map that shows if that a term has a value in the input record.
      */
    def toPresenceMap(record: Array[String]): Map[Term, Long] = {
      terms.zip(record).map({ case (term, value) => {
        (term, if (value == null || value.size == 0) 0L else 1L)
      }
      }).toMap
    }
  }


  implicit class RowConversion(val row: Row) {
    /**
      * Converts a Row into a Array(String) for the columns specified in the columns parameter.
      */
    def toArray(columns: Array[String]): Array[String] = {
      columns.foldLeft(List.empty[String]){ (acc, k) => acc ::: List(row.getString(row.fieldIndex(k)))}.toArray
    }
  }

}
