package org.gbif.validation

import java.io.{FileOutputStream, File, FileNotFoundException}
import java.net.{URL, URI}
import java.nio.file.Paths

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi._
import dispatch.Http
import dispatch._
import org.gbif.validation.api.model.ValidationResult.RecordsValidationResourceResultBuilder
import scala.concurrent.ExecutionContext.Implicits.global
import org.gbif.validation.api.model.{FileFormat, ValidationProfile, ValidationResult}
import org.gbif.validation.collector.{InterpretedTermsCountCollector, TermsFrequencyCollector}
import org.gbif.validation.evaluator.EvaluatorFactory
import org.gbif.validation.tabular.single.SimpleValidationCollector
import org.gbif.validation.util.TempTermsUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  *  Runs the data validation on an HDFS file.
  */
class DataValidationClient(val conf: ValidationSparkConf) {

  val log = LoggerFactory.getLogger(classOf[DataValidationClient])

  private var scalaClient: LivyScalaClient = null

  /**
    *  Initializes the Scala client with the given url in the conf object and uploads the specified jar files.
    */
  def init(): Unit = {
    scalaClient = new LivyClientBuilder(false).setURI(new URI(conf.livyServerUrl)).build().asScalaClient
    //uploadRelevantJarsForJobExecution()
    conf.jars.split(",").foreach(jar => uploadJar(jar))
  }

  /**
    *  Uploads the Scala-API Jar and the examples Jar from the target directory.
    *
    *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
    */
  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    //val exampleAppJarPath = getSourcePath(this)
    val scalaApiJarPath = getSourcePath(scalaClient)
    //uploadJar(exampleAppJarPath)
    uploadJar(scalaApiJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  private def uploadJar(path: String) = {
    def getOrDownload(path: String): File = {
      if(path.startsWith("http")) {
        val redirectResp = Http(url(path) > (res => res))
        val location = redirectResp().getHeader("Location")
        val jarFile = new File(conf.workingDir, location.substring(location.lastIndexOf('/') + 1))
        val download = Http(url(location) >  as.File(jarFile))
        download()
        jarFile
      } else {
        new File(path)
      }
    }

    val file = getOrDownload(path)
    if (!file.isDirectory) {
      val uploadJarFuture = scalaClient.uploadJar(file)
      Await.result(uploadJarFuture, 1000 second) match {
        case null => log.info("Successfully uploaded {}",file.getName)
      }
    }

  }

  /**
    * Processes the data file.
    *
    * @param dataFile data file to be processed data read by the Spark job.
    */
  def processDataFile(dataFile: String): ScalaJobHandle[ValidationResult.RecordsValidationResourceResult] = {
    //url is copied to a string variable to avoid serialization errors
    val gbifApiUrl = conf.gbifApiUrl
    scalaClient.submit { context =>
      val log = LoggerFactory.getLogger(classOf[DataValidationClient])

      val data = context.sqlctx.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "false") // Automatically infer data types
        .load(dataFile).cache()

      val columns = data.columns
      val terms  = TempTermsUtils.buildTermMapping(columns).toList
      val interpretedTermsCountCollector = new InterpretedTermsCountCollector(terms.asJava,true)
      val metricsCollector = new TermsFrequencyCollector(terms.asJava, true)
      val validationCollector  = new SimpleValidationCollector(SimpleValidationCollector.DEFAULT_MAX_NUMBER_OF_SAMPLE)
      val cnt = data.count()

      //This is a bit of duplication: runs all the processing
      data.map(row => columns.foldLeft(List.empty[String]){ (acc, k) => acc ::: List(row.getString(row.fieldIndex(k)))}.toArray)
          .map( record  => {metricsCollector.collect(record);record})
          .zipWithIndex()
          .mapPartitions( partition => {
            val evaluator  = new EvaluatorFactory(gbifApiUrl).create(terms.asJava)
            val newPartition = partition.map( {case(record,idx) => {
              evaluator.evaluate(idx,record)
            }}).toList
          // consumes the iterator
          newPartition.iterator
        }).foreach( result => {validationCollector.collect(result);interpretedTermsCountCollector.collect(result)})

      RecordsValidationResourceResultBuilder.of("", cnt)
        .withIssues(validationCollector.getAggregatedCounts, validationCollector.getSamples)
        .withTermsFrequency(metricsCollector.getTermFrequency)
        .withInterpretedValueCounts(interpretedTermsCountCollector.getInterpretedCounts).build
    }
  }

  /**
    * Stops the client.
    */
  def stop(): Unit = {
    if (scalaClient != null) {
      scalaClient.stop(true)
      scalaClient = null
    }
  }
}