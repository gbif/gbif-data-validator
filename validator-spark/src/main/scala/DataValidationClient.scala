import java.io.{File, FileNotFoundException}
import java.net.URI

import org.gbif.validation.api.model.RecordEvaluationResult
import org.gbif.validation.util.TempTermsUtils
import org.slf4j.LoggerFactory

import scala.collection.immutable.List
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi._

import org.gbif.validation.evaluator.EvaluatorFactory

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
    uploadRelevantJarsForJobExecution()
    conf.jars.split(":").foreach(jar => uploadJar(jar))
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
    val file = new File(path)
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
  def processDataFile(dataFile: String): ScalaJobHandle[Array[RecordEvaluationResult]] = {
    //url is copied to a string variable to avoid serialization errors
    val gbifApiUrl = conf.gbifApiUrl
    scalaClient.submit { context =>
      val log = LoggerFactory.getLogger(classOf[DataValidationClient])

      val data = context.sqlctx.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "false") // Automatically infer data types
        .load(dataFile).cache();

      val columns = data.columns
      //This is a bit of duplication: runs all the processing
      data.rdd.zipWithIndex().mapPartitions( partition => {
            val occEvaluator  = new EvaluatorFactory(gbifApiUrl).create(TempTermsUtils.buildTermMapping(columns))
            val newPartition = partition.map( { case(record,idx) => {
              val values = columns.foldLeft(List.empty[String]){ (acc, k) => acc ::: List(record.getString(record.fieldIndex(k)))}.toArray
              val result = occEvaluator.evaluate(idx, values)
              result
          }}).toList
          // consumes the iterator
          newPartition.iterator
        }).collect()
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
