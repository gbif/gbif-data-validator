package org.gbif.validation.conversion

import java.util
import java.util.HashMap

import org.apache.spark.sql.Row
import org.gbif.dwc.terms.Term
import scala.collection.JavaConversions._


/**
  * Utility to convert between java and scala maps.
  */
object UtilConversions {

  /**
    * Implicit to operate over maps with AnyVal values.
    */
  implicit class ValueMapConversion[K, V <: AnyVal](val m: Map[K, V])
  {
    /**
      * Converts a scala util.Map into a Java mutable map.
      * This conversion is needed when serializing maps in Kryo.
      */
    def toMutableJavaMap[R <: AnyRef](converter: V => R): java.util.Map[K, R]  =
    {
      val newMap: HashMap[K, R] = new HashMap(m.size)
      m.foreach({ case (key, value) => {
        newMap.put(key, converter(value))
      }})
      newMap
    }
  }

  /**
    *  Accumulate operation on numeric values maps.
    */
  implicit class MapAccumulable[K, V <: AnyVal](val m: Map[K, V])(implicit n: Numeric[V])
  {
    def acc(m2: Map[K, V]): Map[K, V] = {
      m ++ (for ((k, v) <- m2) yield (k -> (n.plus(v, m.getOrElse(k, n.zero)))))
    }
  }


  /**
    * Implicit to operate over maps with long values.
    */
  implicit class LongValueMapConversion[K](val m: Map[K, Long])
  {

    /**
      * Converts a scala util.Map into a Java mutable map.
      * This conversion is needed when serializing maps in Kryo.
      */
    def toMutableJavaValueLongMap: java.util.Map[K, java.lang.Long]  =
    {
      m.toMutableJavaMap(long2Long)
    }

    /**
      * Adds values of m2 to m1.
      */
    def accumulate(m2: Map[K, Long]): Map[K, Long] = {
      m ++ (for ((k, v) <- m2) yield (k -> (v + m.getOrElse(k, 0L))))
    }
  }


  /**
    * Converts an scala Map[_,List] into a mutable java Map[_,List].
    */
  implicit  class MutableMapListJava[K,L](val m: Map[K, List[L]]) {

    def toMapListJava : java.util.Map[K, java.util.List[L]] = {
      val newMap: HashMap[K, java.util.List[L]] = new HashMap(m.size)
      m.foreach({ case (key, l) => {
        newMap.put(key, new util.ArrayList(l))
      }
      })
      newMap
    }
  }

  /**
    * Common operations on list of terms.
    */
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

  /**
    * Common operations on list of terms.
    */
  implicit class ListAddIf[T](val l: Option[List[T]]) {

    /**
      * Converts a list of terms and values into a map that shows if that a term has a value in the input record.
      */
    def addIf(element: T, condition: Boolean): List[T] = {
      val listValue = l.getOrElse(List.empty)
      if (condition) {
        listValue :+ element
      }
      listValue
    }
  }


  /**
    * Utility implicit to translate Rows into array of strings.
    */
  implicit class RowConversion(val row: Row) {
    /**
      * Converts a Row into a Array(String) for the columns specified in the columns parameter.
      */
    def toArray(columns: Array[String]): Array[String] = {
      columns.foldLeft(List.empty[String]){ (acc, k) => acc ::: List(row.getString(row.fieldIndex(k)))}.toArray
    }
  }

}
