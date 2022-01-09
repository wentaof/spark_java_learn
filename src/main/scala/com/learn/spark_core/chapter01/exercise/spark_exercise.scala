package com.learn.spark_core.chapter01.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/6 15:20
  * @Version 1.0.0
  * @Description TODO
  */
object spark_exercise {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test07").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filepath = "F:\\学习资料\\尚硅谷-spark\\资料\\spark-core数据\\user_visit_action.txt"
    val rdd = sc.textFile(filepath)
      .map(line => line.replaceAll("null", ""))
      .map(_.split("_"))
      .map(a => {
        UserVisitAction(a(0), a(1).toLong, a(2), a(3).toLong, a(4), a(5), a(6).toLong, a(7).toLong, a(8), a(9), a(10), a(11), a(12).toLong)
      })
    //    方案1
//    fangan_one(sc, rdd)
//    println("--------------")
//    fangan_two(rdd)
//    //    方案3
//    println("--------------")
//    val c_mall_accumulate = new mall_accumulate()
//    fangan_three(sc, rdd, c_mall_accumulate)

    //需求2:
    //  分组求topn
    //    xuqiu2(rdd, c_mall_accumulate)

    sc.stop()
  }

  private def xuqiu2(rdd: RDD[UserVisitAction], c_mall_accumulate: mall_accumulate) = {
    val top10_types = c_mall_accumulate.value.map(_._1).toList
    val result2_temp = rdd.filter(x => top10_types.contains(x.click_category_id))
      .map(x => (x.click_category_id + "_" + x.session_id, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1.split("_")(0), x._1.split("_")(1), x._2))

      //    result2_temp.foreach(println)
      //
      //    val result2 = result2_temp
      .groupBy(_._1)
      .flatMap(x => {
        val k = x._1
        //        val top10 = x._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(10).map(item =>(item._1,item._2,item._3))
        val top10 = x._2.toList.sortWith(_._3 > _._3).take(10).map(item => (item._1, item._2, item._3))
        top10
      }).collect()
      .sortBy(_._1)


    result2_temp.foreach(println)
    //    result2.foreach(println)
  }

  private def fangan_three(sc: SparkContext, rdd: RDD[UserVisitAction], c_mall_accumulate: mall_accumulate) = {
    sc.register(c_mall_accumulate)

    rdd.foreach(x => {
      c_mall_accumulate.add(x.click_category_id, x.order_category_ids, x.pay_category_ids)
    }
    )

    val result = c_mall_accumulate.value.toList
    sc.parallelize(result)
      .collect()
      .sortWith(_._2 > _._2)
      .take(10)
//      .foreach(println)
  }

  private def fangan_two(rdd: RDD[UserVisitAction]) = {
    //    方案2 一次性统计每个品类点击的次数，下单的次数和支付的次数：

    val category_type = rdd.flatMap(r => {
      val result_list = mutable.ListBuffer[(Long, String)]()
      //点击
      if (r.click_category_id != -1) {
        result_list.append((r.click_category_id, "click"))
      }
      //下单
      if (r.order_category_ids != "") {
        result_list ++= (r.order_product_ids.split(",").map(x => (x.toLong, "order")))
      }
      //支付
      if (r.pay_category_ids != "") {
        result_list ++= (r.pay_category_ids.split(",").map(x => (x.toLong, "pay")).toList)
      }
      result_list
    })
      .map(x => (x._1 + "_" + x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1.split("_")(0), (x._1.split("_")(1), x._2)))
      .groupByKey()
      .map(x => (x._1, x._2.toList))
      .map { case (category, type_cnt) => {
        //    总分 综合排名 = 点击数*20%+下单数*30%+支付数*50%
        var score = 0.0
        for (t <- type_cnt) {
          if (t._1 == "click") {
            score += t._2 * 0.2
          }
          if (t._1 == "order") {
            score += t._2 * 0.3
          }
          if (t._1 == "pay") {
            score += t._2 * 0.5
          }
        }
        (category, score)
      }
      }
      .collect() //把排序放到driver端来进行全局排序
      .sortWith(_._2 > _._2)
      .take(10)

    category_type.foreach(println)
  }

  private def fangan_one(sc: SparkContext, rdd: RDD[UserVisitAction]) = {
    // 需求1 ：Top10  热门品类
    //      实现方案1:（品类，点击总数）（品类，下单总数）（品类，支付总数）
    rdd.cache()
    val pinlei_dianji_cnt: collection.Map[Long, Long] = rdd.filter(x => x.click_category_id != None)
      .map(x => (x.click_category_id, 1))
      .countByKey()

    val pinlei_dingdan_cnt = rdd.filter(_.order_category_ids != "")
      .flatMap(_.order_category_ids.split(","))
      .map(x => (x.toLong, 1))
      .countByKey()
    println(pinlei_dingdan_cnt)
    val pinlei_zhifu_cnt = rdd.filter(_.pay_category_ids != "")
      .flatMap(_.pay_category_ids.split(","))
      .map(x => (x.toLong, 1))
      .countByKey()


    //    总分 综合排名 = 点击数*20%+下单数*30%+支付数*50%
    val list1 = pinlei_dianji_cnt.keys.toSet
    val list2 = pinlei_dingdan_cnt.keys.toSet
    val list3 = pinlei_zhifu_cnt.keys.toSet
    val all_types = list1 ++: list2 ++: list3

    val result = mutable.SortedMap[Double, String]()
    for (type_a <- all_types) {
      //      map在获取值的时候,使用getorelse 必须使用同种数据类型,否则会报错,因为类型不统一
      val score_dianji = pinlei_dianji_cnt.getOrElse(type_a, 0L) * 0.2
      val score_dingdan = pinlei_dingdan_cnt.getOrElse(type_a, 0L) * 0.3
      val score_zhifu = pinlei_zhifu_cnt.getOrElse(type_a, 0L) * 0.5
      result.put(score_dianji + score_dingdan + score_zhifu, result.getOrElse(score_dianji + score_dingdan + score_zhifu, "") + type_a + "_")
    }
    val result_lsit = result.toList.reverse
    val result_rdd = sc.parallelize(result_lsit)
    println(result_lsit)
    //    result_lsit.flatMap(_._2.split("_")).filter(_ != "-1").take(10).foreach(println)
    result_lsit.flatMap(x => {
      x._2.split("_").map((x._1, _))
    })
      .filter(_._2 != "-1")
      .take(10)
      .foreach(x => println((x._2, x._1)))
  }
}

class mall_accumulate extends AccumulatorV2[(Long, String, String), mutable.Map[Long, Double]] {
  var map = mutable.Map[Long, Double]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(Long, String, String), mutable.Map[Long, Double]] = new mall_accumulate()

  override def reset(): Unit = map.clear()

  //  分区内
  override def add(in: (Long, String, String)): Unit = {
    //点击数据
    if (in._1 != -1) {
      val k = in._1
      map.put(k, map.getOrElse(k, 0.0) + 0.2)
    }
    //订单数据
    if (in._2 != "") {
      val types = in._2
      for (t <- types.split(",")) {
        val k = t.toLong
        map.put(k, map.getOrElse(k, 0.0) + 0.3)
      }
    }
    //支付数据
    if (in._3 != "") {
      val types = in._3
      for (t <- types.split(",")) {
        val k = t.toLong
        map.put(k, map.getOrElse(k, 0.0) + 0.5)
      }
    }
  }

  //分区间
  override def merge(other: AccumulatorV2[(Long, String, String), mutable.Map[Long, Double]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2)(
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0.0) + kv._2
        innerMap
      }
    )
  }

  override def value: mutable.Map[Long, Double] = map
}