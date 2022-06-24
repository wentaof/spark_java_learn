package com.learn.spark_streaming.chapter01

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 20:50
  * @Version 1.0.0
  * @Description TODO
  */
object read_kafka {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("read_kafka").setMaster("local")
    //创建sparkstreaming环境
    val ssc = new StreamingContext(conf,Seconds(2))
    //定义kafka参数
    val kafkaParam = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> "group_spark",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.198.132:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //定义topic
    val topic = Array("xiaoshuai")
    //读取kafka数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topic,kafkaParam))
    //计算wordcount
    kafkaDstream.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()


  }
}
