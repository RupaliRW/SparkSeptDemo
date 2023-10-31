package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object DFDemo extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","DFSeptDemo")
  sparkConf.set("spark.master","local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()


 // val orderdf = spark.read.option("header",true).option("inferSchema",true).csv("D:/Demos/input/orders.csv")


  //Developer can providec schema while reading a file
  val ordersDDL ="orderid Int,orderdate String, custid Int,status String"
// val orderdf = spark.read.option("header",true).schema(ordersDDL).csv("D:/Demos/input/orders.csv")
  //orderdf.show(2)


  //way 2
  val orderSchema = StructType(List(StructField("orderid",IntegerType,true),StructField("orderdate",StringType,true),
    StructField("custid",IntegerType,true),StructField("status",StringType,true)))
  val orderdf = spark.read.option("header",true).schema(orderSchema).csv("D:/Demos/input/orders.csv")
  orderdf.show(5)

  //processing
  val df1 = orderdf.where("custid<10000")
  df1.cache()
  df1.persist(StorageLevel.MEMORY_AND_DISK)

 // orderdf.where("custid<10000").groupBy("status").agg(functions.min("orderid"),functions.max("orderid")).show(5)

 // println("min  orderid id " + orderdf.agg(functions.min("orderid")).head().get(0))

  //add column with constant value
  //orderdf.withColumn("discount",lit(100)).show(10)

  //update orderid+10
  val finaldf = orderdf.withColumn("neworderid",col("orderid")+10)

  //finaldf.write.format("csv").mode(SaveMode.Overwrite).option("path","D:/Demos/output/final").save()

    //finaldf.write.csv("D:/Demos/output/final")

  //repartition and coalesce

  //finaldf.repartition(1).write.format("csv").mode(SaveMode.Overwrite).option("path","D:/Demos/output/final").save()
  //finaldf.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).option("path","D:/Demos/output/final3").save()


finaldf.createOrReplaceTempView("table1")
  spark.sql("select * from table1 where orderid<10").show()







}
