package com.learn.spark_sql

import com.learn.spark_core.school.bean.Score
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/21 19:58
  * @Version 1.0.0
  * @Description TODO
  */
object chpater01 extends main_obj {
  def main(args: Array[String]): Unit = {
    val score_ds: Dataset[String] = spark.read.textFile("D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_core\\school\\files\\score.txt")

    import spark.implicits._
    val score_df: DataFrame = score_ds.map(_.split(" "))
      .map(x => Score(x(0), x(1), x(2).toInt))
      .toDF()
    score_df.persist()
    score_df.createOrReplaceTempView("score")
    //udf
    spark.udf.register("score_to_level",score_to_level _)
//    val score_to_level_udf = udf(score_to_level _)
//    spark.sql("select * from score limit 10").show()
//    spark.sql("select *,score_to_level(s_score) from score limit 10").show()
//    score_df.withColumn("level",score_to_level_udf(col("s_score"))).show()

    //udaf
//    从 Spark3.0 版本      后，UserDefinedAggregateFunction 已经不推荐使用了。可以统一采用强类型聚合函数    Aggregator






  }

  def score_to_level(score : Int): String ={
    var result = "不及格"
    if(score >= 60){
      result = "及格"
    }
    result
  }

}
