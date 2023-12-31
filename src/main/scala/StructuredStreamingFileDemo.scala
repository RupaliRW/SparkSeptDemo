package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.streaming.Trigger


object StructuredStreamingFileDemo extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFSeptDemo")
  sparkConf.set("spark.master", "local[1]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
  sparkConf.set("spark.sql.streaming.schemaInference", "true")

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ordersdf = spark.readStream.format("json").option("path", "Input").load()

  ordersdf.createOrReplaceTempView("order")

  val out = spark.sql("select * from order where order_status='ON_HOLD'")

   out.writeStream.outputMode("complete").format("json").option("path", "output").
    option("checkpointLocation", "checkpointloc2").trigger(Trigger.ProcessingTime("5 seconds")).start().awaitTermination()


}
