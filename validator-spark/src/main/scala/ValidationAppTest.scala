import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._

object ValidationAppTest {

  val log = LoggerFactory.getLogger("ValidationAppTest")
  /**
    * Main method of the WordCount App. This method does the following
    * - Validate the arguments.
    * - Initializes the scala client of livy.
    * - Uploads the required livy and app code jar files to the spark cluster needed during runtime.
    * - Executes the streaming job that reads text-data from socket stream, tokenizes and saves
    *   them as dataframes in JSON format in the given output path.
    * - Executes the sql-context job which reads the data frames from the given output path and
    * and returns the word with max count.
    *
    */
  def main(args: Array[String]): Unit = {

    val  conf = new ValidationSparkConf("http://devgateway-vh.gbif.org:8998/","validator-spark/target/validator-spark-0.1-SNAPSHOT.jar:validator-core/target/validator-core-0.1-SNAPSHOT.jar:/Users/fmendez/.m2/repository/com/cloudera/livy/livy-client-http/0.3.0-SNAPSHOT/livy-client-http-0.3.0-SNAPSHOT.jar","http://api.gbif-dev.org/v1/")
    val client = new DataValidationClient(conf)
    try {
      client.init()
      val handle = client.processDataFile("hdfs:///user/fmendez/0008759-160822134323880.csvar")
      Await.result(handle, 100000 second)
      log.info("Data Validation Result::{}", Await.result(handle, 10000 second))
    } catch {
      case e: Throwable => log.error("Error initiating client", e)
    } finally {
      client.stop()
    }
  }
}
