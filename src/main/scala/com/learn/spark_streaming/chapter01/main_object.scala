package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 22:39
  * @Version 1.0.0
  * @Description TODO
  */
object main_object {

  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("read_kafka").setMaster("local[*]")
    //创建sparkstreaming环境
    val ssc = new StreamingContext(conf,Seconds(2))


    ssc.start()
    ssc.awaitTermination()
  }

}
