package com.learn.spark_streaming.project.black_list

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.learn.spark_streaming.project.utils.{Kafka_utils, click_info, mysql_utils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/20 14:16
  * @Version 1.0.0
  * @Description 广告点击量实时统计
  */
object ad_realtime_click {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ad_realtime_click")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val kafka_ds: InputDStream[ConsumerRecord[String, String]] = Kafka_utils.getKafkaDStream(ssc)

    val fmt = new SimpleDateFormat("yyyy-MM-dd")

    kafka_ds.map(line => {
      val strings: Array[String] = line.value().split(" ")
      val date = new Date(strings(0).toLong)
      val date_str: String = fmt.format(date)
      click_info(date_str, strings(1), strings(2), strings(3).toInt, strings(4).toInt)
    })
      .map(x=>((x.day,x.area,x.city,x.adid),1))
      .reduceByKey(_ + _)
      .foreachRDD(_.foreachPartition(x=>{
        val conn: Connection = mysql_utils.getconnection()
        for (elem <- x) {
          mysql_utils.upsert_area_city_ad_count(conn,elem)
        }
        conn.close()
      }))

    ssc.start()
    ssc.awaitTermination()

  }
}
