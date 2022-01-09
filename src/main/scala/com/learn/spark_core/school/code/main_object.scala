package com.learn.spark_core.school.code

import org.apache.spark.{SparkConf, SparkContext}
import com.learn.spark_core.school.bean._
import org.apache.spark.rdd.RDD

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/9 17:48
  * @Version 1.0.0
  * @Description TODO
  */
class main_object() {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql-50")
  val sc = new SparkContext(conf)
  private val file_student = "D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_core\\school\\files\\student.txt"
  private val file_course = "D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_core\\school\\files\\course.txt"
  private val file_teacher = "D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_core\\school\\files\\teacher.txt"
  private val file_score = "D:\\coder\\learn\\spark_learn\\src\\main\\scala\\com\\learn\\spark_core\\school\\files\\score.txt"

  val rdd_student = sc.textFile(file_student).map(_.split(" ")).map(x => Student(x(0), x(1), x(2), x(3)))
  val rdd_course = sc.textFile(file_course).map(_.split(" ")).map(x => Course(x(0), x(1), x(2)))
  val rdd_teacher = sc.textFile(file_teacher).map(_.split(" ")).map(x => Teacher(x(0), x(1)))
  val rdd_score = sc.textFile(file_score).map(_.split(" ")).map(x => Score(x(0), x(1), x(2).toInt))


  def rdd_println[T <: Any](rdd:RDD[T]): Unit ={
    rdd.foreach(println)
  }

}
