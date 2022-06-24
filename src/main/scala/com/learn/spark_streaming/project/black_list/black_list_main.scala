package com.learn.spark_streaming.project.black_list

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.learn.spark_streaming.project.utils.{Kafka_utils, click_info, mysql_utils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/19 11:12
  * @Version 1.0.0
  * @Description 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。
  *
  *              1）读取 Kafka 数据之后，并对 MySQL 中存储的黑名单数据做校验；
  *              2）校验通过则对给用户点击广告次数累加一并存入 MySQL；
  *              3）在存入 MySQL 之后对数据做校验，如果单日超过 100 次则将该用户加入黑名单。
  *              array += timestamp + " " + area + " " + city + " " + userid + " " + adid
  */
object black_list_main {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("black_list_main")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[ConsumerRecord[_, _]]))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val kafka_ds = Kafka_utils.getKafkaDStream(ssc)

    val fmt = new SimpleDateFormat("yyyy-MM-dd")

    kafka_ds.map(line => {
      val strings: Array[String] = line.value().split(" ")
      val date = new Date(strings(0).toLong)
      val date_str: String = fmt.format(date)
      click_info(date_str, strings(1), strings(2), strings(3).toInt, strings(4).toInt)
    })
      .map(x => ((x.day, x.userid, x.adid), 1))
      .reduceByKey(_ + _)
      .repartition(2)
      .foreachRDD(rdd => {
        rdd.foreachPartition(x => {
          val conn = mysql_utils.getconnection()
          // 处理每一条数据
          for (item <- x) {
            upsert_mysql_day_userid_adid(conn, item)
          }
          conn.close()
        })
      }
      )

    ssc.start()
    ssc.awaitTermination()
  }

  def upsert_mysql_day_userid_adid(conn: Connection, row: ((String, Int, Int), Int)): Any = {
    val where_str = s"where dt='${row._1._1}' and userid = ${row._1._2} and adid = ${row._1._3}"
    // 查询黑名单表 如果cnt > 100 就不用操作了,直接加入
    val query_black_list = s"select userid from black_list where userid = ${row._1._2}"
    if (mysql_utils.query_execute_long(query_black_list, conn) != -1) {
      println(s"${row._1._2} 已经加入黑名单!")
      return null
    }

    //    查询数据库 获取原始的数量
    val query_sql = s"select count from user_ad_count ${where_str};"
    var src_cnt = mysql_utils.query_execute_long(query_sql, conn)
    if (src_cnt == -1) {
      //表中没数,需要初始化
      val sql = s"INSERT INTO `spark2020`.`user_ad_count` (`dt`, `userid`, `adid`, `count`) VALUES ('${row._1._1}', ${row._1._2}, ${row._1._3}, 0);"
      mysql_utils.upsert_execute(sql, conn)
      src_cnt = 0
    }

    //    获取当前的数量
    val update_count = src_cnt + row._2
    if (update_count > 100) {
      val insert_sql = s"insert into black_list values(${row._1._2})"
      mysql_utils.upsert_execute(insert_sql, conn)
    } else {
      val update_sql = s"update user_ad_count set count = ${update_count} ${where_str};"
      mysql_utils.upsert_execute(update_sql, conn)
    }

  }

}



