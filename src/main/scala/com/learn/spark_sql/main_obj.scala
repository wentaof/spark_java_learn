package com.learn.spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/21 19:58
  * @Version 1.0.0
  * @Description TODO
  */
class main_obj {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("main_obj")
    .master("local")
//    .enableHiveSupport()
    .getOrCreate()
  private val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")


}
