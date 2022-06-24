package com.learn.spark_streaming.project.black_list


import java.text.SimpleDateFormat
import java.util.Date

import com.learn.spark_streaming.project.utils.{Kafka_utils, click_info}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/20 14:44
  * @Version 1.0.0
  * @Description 最近一小时广告点击量
  */
object cal_last_1hour {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ad_realtime_click")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[ConsumerRecord[_, _]]))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val kafka_ds: InputDStream[ConsumerRecord[String, String]] = Kafka_utils.getKafkaDStream(ssc)

    val fmt = new SimpleDateFormat("HH:mm")

    kafka_ds
      .map(line => {
      val strings: Array[String] = line.value().split(" ")
      val date = new Date()
      val hhmm: String = fmt.format(date)
      click_info(hhmm, strings(1), strings(2), strings(3).toInt, strings(4).toInt)
    })
      .window(Minutes(2))  //window 要放到map之后,否则会报错.但是不影响计算
      .map(x=>((x.day,x.adid),1))
      .reduceByKey(_ + _)
      .foreachRDD(_.foreachPartition(x=>{
        for (elem <- x) {
          println(elem)
        }
      }))

    ssc.start()
    ssc.awaitTermination()
  }
}
