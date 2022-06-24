package com.learn.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 19:40
  * @Version 1.0.0
  * @Description TODO
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val line_stream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    line_stream.flatMap(_.split(" "))
        .map(x=>(x,1))
        .reduceByKey(_ + _)
        .print()


    ssc.start()
    ssc.awaitTermination()

  }
}
