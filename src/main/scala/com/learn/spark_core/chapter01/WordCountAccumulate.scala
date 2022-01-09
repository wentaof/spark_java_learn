package com.learn.spark_core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection._

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/6 14:33
  * @Version 1.0.0
  * @Description TODO
  */
class WordCountAccumulate extends AccumulatorV2[String, mutable.Map[String, Int]] {
  var map = mutable.Map[String, Int]()

  //  累加器是否为初始状态
  override def isZero: Boolean = {
    map.isEmpty
  }

  //复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new WordCountAccumulate()
  }

  //重置累加器
  override def reset(): Unit = {
    map.clear()
  }

  //像累加器中添加数据
  override def add(word: String): Unit = {
    map(word) = map.getOrElse(word, 0) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = map
    val map2 = other.value

    //合并两个map
    map = map1.foldLeft(map2)(
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0) + kv._2
        innerMap
      }
    )
  }

  override def value: mutable.Map[String, Int] = map
}


object WordCountAccumulate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test07").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\desktop\\learn\\spark\\spark-core\\wordcount.txt")
    val rdd_str: RDD[String] = rdd.flatMap(x => x.split(" "))
    //    rdd_str.foreach(println)
    //创建累加器
    val map_acc = new WordCountAccumulate()
    //注册累加器
    sc.register(map_acc)

    rdd_str.foreach(x => {
      map_acc.add(x)
    })

    println(map_acc.value.toArray.toBuffer)
  }
}