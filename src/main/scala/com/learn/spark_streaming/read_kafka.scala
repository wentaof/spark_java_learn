package com.learn.spark_streaming



import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/14 17:06
  * @Version 1.0.0
  * @Description TODO
  */
object read_kafka {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("read_kafka").setMaster("local[2]")
//    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().master("local").appName("read_kafka").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Milliseconds(200))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.198.132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "xiaoshuai_spark_1",
      "auto.offset.reset" -> "earliest", //earliest latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Array("xiaoshuai")
    val ds  = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String,String](topic,kafkaParams))
    ds.map(record=>(record.key(),record.value(),record.timestamp(),System.currentTimeMillis())).foreachRDD(x=>x.foreach(y=>{ println(y) }))
    ssc.start()
    ssc.awaitTermination()

    //    val lines: DStream[String] = ds.map(cr => {
//      println(s"message key = ${cr.key()}")
//      println(s"message value = ${cr.value()}")
//      cr.value()
//    })
//
//    val words: DStream[String] = lines.flatMap(line => {
//      line.split(" ")
//    })
//    val pairWords: DStream[(String, Int)] = words.map(word => {
//      (word, 1)
//    })
//    val result = pairWords.reduceByKey((v1, v2) => {
//      v1 + v2
//    })
//    result.print()

    //保证业务逻辑处理完成的情况下，异步将当前批次offset提交给kafka,异步提交DStream需要使用源头的DStream
//    ds.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      // some time later, after outputs have completed
//      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) // 异步向Kafka中提交消费者offset
//    }



    //
    //    stream.map(x=> {
    //      println(1111)
    //      (11,1)
    //    }).count()



  }
}
