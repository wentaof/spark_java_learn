package com.learn.spark_streaming.chapter01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 23:02
  * @Version 1.0.0
  * @Description TODO
  */
object updateStateByKey_test {
  def main(args: Array[String]): Unit = {
    //定义更新状态方法,参数values为当前批次单词频度,status为以往批次单词频度
    val updateFunc =(values:Seq[Int], state: Option[Int])=>{
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }



    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("read_kafka").setMaster("local[*]")
    //创建sparkstreaming环境
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("./ck")
    ssc.socketTextStream("localhost",9999).map((_,1))
        .updateStateByKey[Int](updateFunc).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
