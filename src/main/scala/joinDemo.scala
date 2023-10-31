package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object joinDemo extends App{


  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFSeptDemo")
  sparkConf.set("spark.master", "local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  val empDF = Seq(("Rupali", "Mumbai"), ("Mohsin", "pune"), ("Devi", "bangalore"), ("Alex", "hyderabad")).toDF("fName", "city")

  val citiesDF = Seq(("Mumbai", "India"), ("pune", "India"), ("bangalore", "India"), ("hyderabad", "India")).toDF("city", "country")

  val joindf = empDF.join(broadcast(citiesDF),empDF.col("city")=== citiesDF.col("city"),"inner")
 val resdf =  joindf.drop(empDF.col("city"))
  resdf.select("city","fName","country").show(3)
}
