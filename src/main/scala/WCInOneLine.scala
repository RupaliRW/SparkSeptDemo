package org.itc.com
import org.apache.spark.SparkContext

object WCInOneLine {
  def main(args: Array[String]): Unit = {

  val sc = new SparkContext()
    val fileRDD = sc.textFile(args(0)).flatMap(x => x.toLowerCase().split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(_._2, false)
  //print final output to screen.
 fileRDD.collect().foreach(println)





  }

}
