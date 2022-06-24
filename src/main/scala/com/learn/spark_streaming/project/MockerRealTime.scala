package com.learn.spark_streaming.project

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/18 20:54
  * @Version 1.0.0
  * @Description TODO
  */
object MockerRealTime {
  /**
    * 模拟的数据
    *
    * 格式 ：timestamp area city userid adid
    * 某个时间点 某个地区 某个城市 某个用户 某个广告
    */
  def generateMockData(): Array[String] = {
    val array: ArrayBuffer[String] = ArrayBuffer[String]()
    val CityRandomOpt = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(2, "上海", "华东"), 30),
      RanOpt(CityInfo(3, "广州", "华南"), 10),
      RanOpt(CityInfo(4, "深圳", "华南"), 20),
      RanOpt(CityInfo(5, "天津", "华北"), 10))
    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {
      val cityInfo: CityInfo = CityRandomOpt.getRandomOpt

      val timestamp: Long = System.currentTimeMillis()
      val area: String = cityInfo.area
      val city: String = cityInfo.city_name
      val userid: Int = 1 + random.nextInt(6)
      val adid: Int = 1 + random.nextInt(6)
      // 拼接实时数据
      array += timestamp + " " + area + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者
    new KafkaProducer[String, String](prop)
  }

  def main(args: Array[String]): Unit = {
    // 获取配置文件 config-spark-streaming-project.properties 中的 Kafka 配置参数
    val config: Properties = PropertiesUtil.load("config-spark-streaming-project.properties")
    val broker: String = config.getProperty("kafka.broker.list")
    val topic = config.getProperty("kafka.topic")
    // 创建 Kafka 消费者
    val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)
    while (true) {
      // 随机产生实时数据并通过 Kafka 生产者发送到 Kafka 集群中
      for (line <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(5000)
    }
  }

}
