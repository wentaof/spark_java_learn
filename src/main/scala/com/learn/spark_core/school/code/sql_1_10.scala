package com.learn.spark_core.school.code

import com.learn.spark_core.school.bean.Score
import org.apache.spark.rdd.RDD

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/9 17:51
  * @Version 1.0.0
  * @Description TODO
  */
object sql_1_10 extends main_object {
  def main(args: Array[String]): Unit = {
    no_print(1)
    //    1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    val score_01 = rdd_score.filter(_.c_id == "01").map(s => (s.s_id, s.s_score))
    val score_02 = rdd_score.filter(_.c_id == "02").map(s => (s.s_id, s.s_score))
    val score_join: RDD[(String, (Int, Int))] = score_01.join(score_02).filter(x => {
      x._2._1 > x._2._2
    })
    rdd_student.map(x => (x.s_id, (x.s_name, x.s_birth, x.s_sex)))
      .rightOuterJoin(score_join)
    //      .foreach(println)
    //结果:
    //(02,(Some((钱电,1990-12-21,男)),(70,60)))
    //(04,(Some((李云,1990-08-06,男)),(50,30)))

    no_print(2)
    // 2 查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    val score_join_down: RDD[(String, (Int, Int))] = score_01.join(score_02).filter(x => {
      x._2._1 < x._2._2
    })
    rdd_student
      .map(x => (x.s_id, (x.s_name, x.s_birth, x.s_sex)))
      .leftOuterJoin(score_join_down)
      .filter(!_._2._2.isEmpty)
    //      .foreach(println)

    no_print(3)
    val rdd_score_60: RDD[(String, Double)] = rdd_score.map(s => (s.s_id, s.s_score))
      .groupByKey()
      .map(x => {
        val s_id = x._1
        val size = x._2.size
        val total_score = x._2.toList.sum
        (s_id, total_score * 1.0 / size)
      })
      .filter(_._2 > 60)

    rdd_student.map(s => (s.s_id, s.s_name))
      .join(rdd_score_60)
      .map(x => (x._1, x._2._1, x._2._2))
    //      .foreach(println)

    no_print(4)
    //查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
    rdd_score.map(s => (s.s_id, s.s_score))
      .groupByKey()
      .map(x => {
        val avg_score = 1.0 * x._2.sum / x._2.size
        (x._1, avg_score)
      })
      .filter(x => x._2 < 60)
      .join(rdd_student.map(x => (x.s_id, x.s_name)))
    //      .foreach(println)

    no_print(5)
    //    查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
    val rdd_sid_totalcourse_totalscore: RDD[(String, (Int, Int))] = rdd_score.map(x => (x.s_id, x.s_score))
      .groupByKey()
      .map(x => {
        val total_course = x._2.size
        val total_score = x._2.sum
        (x._1, (total_course, total_score))
      })
    rdd_student.map(x => (x.s_id, x.s_name))
      .leftOuterJoin(rdd_sid_totalcourse_totalscore)
      .map(x => {

        val result = if (x._2._2.isEmpty) {
          (x._1, x._2._1, None, None)
        } else {
          (x._1, x._2._1, x._2._2.get._1, x._2._2.get._2)
        }
        result
      })
    //      .foreach(println)

    no_print(6)
    //    6、查询"李"姓老师的数量
    val l: Long = rdd_teacher.filter(t => t.t_name.startsWith("李")).count()
    //    println(l)

    no_print(7)
    //7、查询学过"张三"老师授课的同学的信息
    var t_id = rdd_teacher.filter(_.t_name == "张三").map(_.t_id).take(1)(0)
    var c_id = rdd_course.filter(_.t_id == t_id).map(_.c_id).take(1)(0)
    var s_ids = rdd_score.filter(_.c_id == c_id).map(_.s_id).distinct().collect()
    //    rdd_student.filter(x=>s_ids.contains(x.s_id)).foreach(println)

    no_print(8)
    //8 查询没学过"张三"老师授课的同学的信息
//    rdd_student.filter(x => !s_ids.contains(x.s_id)).foreach(println)

    no_print(9)
//    9、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
    val s_id2 = rdd_score.filter(x => List("01", "02").contains(x.c_id)).map(x => (x.s_id, x.c_id)).groupByKey()
      .filter(_._2.size == 2)
      .map(_._1).distinct().collect()
//    rdd_student.filter(x=>s_id2.contains(x.s_id)).foreach(println)

    no_print(10)
//10、查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
    val s_id_01 = rdd_score.filter(_.c_id == "01").map((_.s_id)).collect()
    val s_id_02 = rdd_score.filter(_.c_id == "02").map((_.s_id)).collect()
    val s_id_10: Array[String] = s_id_01.diff(s_id_02)

    rdd_student.filter(x=>s_id_10.contains(x.s_id)).foreach(println)

  }
}
