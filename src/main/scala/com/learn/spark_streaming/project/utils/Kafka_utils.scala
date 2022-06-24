package com.learn.spark_streaming.project.utils

import java.io.{FileReader, InputStreamReader}
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/19 14:01
  * @Version 1.0.0
  * @Description TODO
  */
object Kafka_utils extends Serializable {
  def getKafkaDStream(ssc:StreamingContext)={
    val prop = new Properties()
    prop.load(new InputStreamReader(new Thread().getContextClassLoader.getResourceAsStream("config-spark-streaming-project.properties"),"utf-8"))
    val topics = prop.getProperty("kafka.topic").split(",")
    val params: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> prop.getProperty("BOOTSTRAP_SERVERS_CONFIG"),
      ConsumerConfig.GROUP_ID_CONFIG -> prop.getProperty("GROUP_ID_CONFIG"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("KEY_DESERIALIZER_CLASS_CONFIG"),
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG"),
    )

    val kafka_ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,params))
    kafka_ds
  }
}
