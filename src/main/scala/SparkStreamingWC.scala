package org.itc.com

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWC extends App{
  val sc = new SparkContext("local[*]","WCstreamingdemo")
  val ssc = new StreamingContext(sc,Seconds(2))

  //1. reading
 val lines =  ssc.socketTextStream("localhost",9998)
  //2. Processing
  val wordCount =  lines.flatMap(x => x.toLowerCase().split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
  //3. output on console
  wordCount.print()
  ssc.start()
  ssc.awaitTermination() //keep application running



}
