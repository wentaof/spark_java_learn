package com.learn.spark_sql

import org.apache.spark.sql.DataFrame

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/22 13:49
  * @Version 1.0.0
  * @Description TODO
  */
object chapter02 extends main_obj {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val user_df: DataFrame = spark.read.json("D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_sql\\user.json")
    user_df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    user_df.show()
  }
}
