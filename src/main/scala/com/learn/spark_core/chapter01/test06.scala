package com.learn.spark_core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/5 21:21
  * @Version 1.0.0
  * @Description TODO
  */
object test06 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 设置检查点路径
    sc.setCheckpointDir("./checkpoint1")
    // 创建一个 RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("D:\\desktop\\learn\\spark\\spark-core\\wordcount.txt")
    // 业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }
    // 增加缓存,避免再重新跑一个 job 做 checkpoint
    wordToOneRdd.cache()
    // 数据检查点：针对 wordToOneRdd 做检查点计算
    wordToOneRdd.checkpoint()
    // 触发执行逻辑
    wordToOneRdd.collect().foreach(println)
  }
}
