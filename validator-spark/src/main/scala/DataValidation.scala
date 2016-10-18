/* SimpleApp.scala */

import java.io.ByteArrayOutputStream
import java.net.URL

import org.apache.spark.repl.SparkIMain
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{JPrintWriter, Results}


object DataValidation {
  private val outputStream = new ByteArrayOutputStream()

  def main(args: Array[String]) {

    val t0 = System.currentTimeMillis();

    val interpreter: ScalaInterpreterSpecific = new ScalaInterpreterSpecific()

    interpreter.start()


    val conf = new SparkConf().setAppName("Simple Application")
                .setMaster("spark://fmendez.gbif.org:7077").set("spark.repl.class.uri", interpreter.sparkIMain.classServerUri)
                .set("spark.submit.deployMode", "client")
                .setJars(Seq("/Users/fmendez/dev/git/gbif/gbif-data-validator/validator-core/target/validator-core-0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    interpreter.doQuietly(
      interpreter.bind("sc", "org.apache.spark.SparkContext", sc, List("""@transient"""))
    )

   val methodRef =
     """
        import org.apache.spark.rdd.RDD
        import org.apache.spark.sql.types.{StringType, StructField, StructType}
        import org.apache.spark.sql.{Row, SQLContext}
        import org.gbif.dwc.terms.Term
        import org.gbif.occurrence.validation.evaluator.OccurrenceEvaluatorFactory
        import org.gbif.occurrence.validation.util.TempTermsUtils


      val sqlContext: SQLContext = new SQLContext(sc)
      val dataFile = "/Users/fmendez/dev/git/gbif/gbif-data-validator/validator-core/src/test/resources/0008759-160822134323880.csvar"
    val data = sc.textFile(dataFile, 20).cache()
    val header = data.first()
    val rawHeader = header.split("\t")
    val terms: Array[Term] = TempTermsUtils.buildTermMapping(header.split("\t"))

    //This creates a schema from the header
    val schema = StructType(rawHeader.map(fieldName ⇒ StructField(fieldName, StringType, true)))

    // this is to ensure that each row has the same number as columns as reported in the header
    //RDD[Row] is the data type expected by the session.createDataFrame
    val rowData: RDD[Row] = data.zipWithIndex().filter({case(_,idx) => idx != 0})
      .map(line => Row.fromSeq(line._1.split("\t").padTo(rawHeader.length,"")))

    val ds = sqlContext.createDataFrame(rowData,schema)

    //Creates the view
    val occDs = ds.registerTempTable("occ")

    //runs a sql statement
    sqlContext.sql("select count(distinct occurrenceid) from occ").collect()

    //This is a bit of duplication: runs all the processing
    val results = data.zipWithIndex().filter( {case(line,idx) => idx != 0})
                       .map({case(line,idx) => (idx,(line.split("\t")))})
                       .mapPartitions( partition => {
                         val occEvaluator  = new OccurrenceEvaluatorFactory("http://api.gbif.org/v1/").create(rawHeader)
                         val newPartition = partition.map( { case(idx,record) => {
                                                                occEvaluator.evaluate(idx, record)}}).toList
                                                                // consumes the iterator, thus calls readMatchingFromDB
                         newPartition.iterator
                       }).collect()
    """
    val resultFlag = interpreter.interpret(methodRef,false)
    val results = interpreter.read("results")
    val t1 = System.currentTimeMillis();


    println("Elapsed time: " + (t1 - t0)/1000 + "s")

  }
}

