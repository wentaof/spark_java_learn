package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 22:28
  * @Version 1.0.0
  * @Description TODO
  */
object transform_test {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("read_kafka").setMaster("local[*]")
    //创建sparkstreaming环境
    val ssc = new StreamingContext(conf,Seconds(2))

    val line_rdd: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
// 个人理解: 有点类似与map,但是又不一样,transform是针对rdd进行操作的,最后返回rdd.
    line_rdd.transform(rdd =>{
      rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    }).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
