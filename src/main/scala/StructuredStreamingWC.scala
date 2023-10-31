package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingWC  extends App{


  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFSeptDemo")
  sparkConf.set("spark.master", "local[1]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

 val linesdf = spark.readStream.format("socket").option("host","localhost").option("port",9997).load()

  /*
  import spark.implicits._
 val wordsdf= linesdf.as[String].flatMap((_.split(" ")))
  val count = wordsdf.groupBy("value").count()
  */

    val wordsDF = linesdf.select(explode(functions.split(linesdf("value"), " ")).alias("word"))
  val count = wordsDF.groupBy("word").count()

 val producer1 = count.writeStream.outputMode("complete").format("console").
   option("checkpoint","checkpointloc1").trigger(Trigger.ProcessingTime("2 seconds")).start().awaitTermination()

  //producer1.awaitTermination()



  /*
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9997")
    .load()

  val wordsDF = df.select(explode(functions.split(df("value"), " ")).alias("word"))
  val count = wordsDF.groupBy("word").count()
  val query = count.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()

*/
}
