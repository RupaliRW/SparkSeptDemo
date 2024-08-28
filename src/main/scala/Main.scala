package org.itc.com

import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")


    val sc = new SparkContext("local[1]","FirstSparkApp");
    val fileRDD = sc.textFile("D:\\Demos\\input\\data.txt")
    //accepts one line and returns many words - 1 to M
    val wordsRDD = fileRDD.flatMap(x => x.toLowerCase().split(" "))
    //word = (word,1) - 1 to 1
    val wordRDD = wordsRDD.map(x => (x,1))
    //aggregation
    val finalOutput = wordRDD.reduceByKey((x,y)=>x+y).sortBy(_._2,false)
    //print final output to screen.
    finalOutput.collect().foreach(println)

    println("Done")


  }
}