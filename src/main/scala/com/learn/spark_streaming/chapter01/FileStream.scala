package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 20:30
  * @Version 1.0.0
  * @Description 使用自定义数据源
  */
object FileStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    val ssc = new StreamingContext(conf,Seconds(4))
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("localhost",9999))
    lineStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
        .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
