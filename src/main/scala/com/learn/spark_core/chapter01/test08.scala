package com.learn.spark_core.chapter01

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.LongAccumulator

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/6 15:09
  * @Version 1.0.0
  * @Description TODO
  */
object test08 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test07").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //    val rdd = sc.makeRDD(List(1,2,3,4,5))
    ////    rdd 不能直接进行广播
    //    val rdd_b: Broadcast[Array[Int]] = sc.broadcast(rdd.collect())
    //    val value: Array[Int] = rdd_b.value
    //    value.foreach(println)

    val rdd3 = sc.makeRDD(Array(("a", "user1", "25"), ("b", "user1", "27"), ("c", "user1", "12"), ("d", "user2", "23"), ("e", "user2", "1"), ("a", "user3", "30")), 2).map(x => {

      val lac = x._1

      val user = x._2

      val cnt = x._3

      (user.toString, lac, cnt.toInt)

    }).take(6)

    for (elem <- rdd3) {
      println("rdd3_"+elem)
    }

    val topK = rdd3.groupBy(item => (item._1)).map(subG => {

      val (usera) = subG._1

      val dayTop1 = subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(1).map(item => item._2 + "," + item._1 + "," + item._3)

      (usera, dayTop1)

    })
    for (elem <- topK) {
      println(s"top3:$elem")
    }

    sc.stop()
  }
}
