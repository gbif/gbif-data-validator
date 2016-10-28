package org.gbif.validation

class ValidationSparkConf (val livyServerUrl: String, val jars: String, val gbifApiUrl: String,
                           val workingDir: String) extends Serializable {

}
