package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DSDemo extends App{


  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFSeptDemo")
  sparkConf.set("spark.master", "local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  case class orderdata (orderid:Int,orderdate:String, custid:Int,status:String)

  val ordersDDL ="orderid Int,orderdate String, custid Int,status String"
 val orderdf = spark.read.option("header",true).schema(ordersDDL).csv("D:/Demos/input/orders.csv")

  import spark.implicits._
  val orderDS = orderdf.as[orderdata]

  orderDS.filter("orderid>10000").show(2)


}
