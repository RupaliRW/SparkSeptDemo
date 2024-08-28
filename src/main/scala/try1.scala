package org.itc.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object try1{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "DFSeptDemo")
    sparkConf.set("spark.master", "local[1]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()



    // Sample DataFrame
    val data = Seq(("A", Some(10), None), ("B", None, Some(30)), ("C", Some(40), Some(50)))
    val columns = Seq("ID", "Value1", "Value2")

    val df = spark.createDataFrame(data).toDF(columns: _*)

    // Show the original DataFrame
    println("Original DataFrame:")
    df.show()

    // Replace null values in "Value1" with values from "Value2" and create a new column "NewColumn"
    val dfReplaced = df.withColumn("NewColumn", coalesce(col("Value1"), col("Value2")))

    // Show the DataFrame after replacing null values
   // println("DataFrame after replacing null values:")
    //dfReplaced.show()

    // Replace null values in "Value1" with values from "Value2" only when "Value1" is null
    val dfReplaced1 = df.withColumn("NewColumn", when(col("Value1").isNull, col("Value2")).otherwise(col("Value2")))

    dfReplaced1.show()
    // Stop Spark session
    spark.stop()
  }
}
