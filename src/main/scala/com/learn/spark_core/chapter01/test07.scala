package com.learn.spark_core.chapter01

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/6 14:08
  * @Version 1.0.0
  * @Description  累加器
  */
object test07 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test07").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5))
    //声明累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(sum.add(_))
    println(s"sum= ${sum.value}")
    sc.stop()
  }
}
