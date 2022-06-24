package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 22:38
  * @Version 1.0.0
  * @Description TODO
  */
object join_test {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("read_kafka").setMaster("local[*]")
    //创建sparkstreaming环境
    val ssc = new StreamingContext(conf,Seconds(10))

    val line1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val line2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9998)

    val line1_ds = line1.flatMap(_.split(" ")).map((_,(1,2)))
    val line2_ds = line2.flatMap(_.split(" ")).map((_,('a','b')))

    line1_ds.join(line2_ds).transform(x=>{
      println("x:"+x)
      x
    }).print()
// spark hadoop spark
// spark hadoop hbase
//    结果:
//    (spark,(1,97))
//    (spark,(1,97))
//    (hadoop,(1,97))


    ssc.start()
    ssc.awaitTermination()
  }
}
