package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 20:00
  * @Version 1.0.0
  * @Description TODO
  */
object RDDStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    val ssc = new StreamingContext(conf,Seconds(4))

    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,false)
    inputStream.map((_,1)).reduceByKey(_ + _).print()
    ssc.start()

    for (elem <- (1 to 5)) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
