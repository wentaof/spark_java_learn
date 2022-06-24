package com.learn.spark_core.school.code

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/12 10:09
  * @Version 1.0.0
  * @Description TODO
  */
object sql_30_40 extends main_object {
  def main(args: Array[String]): Unit = {
    no_print(31)
    //    31、查询1990年出生的学生名单
    //    rdd_student.filter(_.s_birth.substring(0,4) == "1990").foreach(println)

    no_print(32)
    //32、查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
    //    rdd_score.groupBy(_.c_id)
    //      .map(x=>{
    //        val scores: List[Int] = x._2.toList.map(_.s_score).toList
    //        val cnt = scores.size
    //        val sum: Int = scores.sum
    //        val avg_score = sum * 1.0 /cnt
    //        (x._1, avg_score)
    //      })
    //      .map(line=>{
    //      new compare_2(line._2,line._1)
    //        (line._2,line._1)
    //    })
    //      .sortByKey(false,1)
    //      .foreach(println)
    //    sc.textFile("D:\\desktop\\a.txt")
    //      .map(x=>{
    //        (x.split(" ")(0), x.split(" ")(1))
    //      })
    //      .map(x=>{
    //        (new compare_2(x._1.toDouble,x._2)
    //          ,x)
    //      })
    //      .sortByKey(true,1)
    //      .map(_._2)
    //      .foreach(println)

    no_print(33)
    //33、查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩
    no_print(34)
    no_print(36)
    //    36、查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数
    val score_70 = rdd_score.filter(_.s_score > 70)
    score_70.map(x => (x.c_id, (x.s_id, x.s_score)))
      .leftOuterJoin(rdd_course.map(x => (x.c_id, x.c_name)))
      .map(x => (x._2._1._1, (x._2._2.get, x._2._1._2)))
      .leftOuterJoin(rdd_student.map(x => (x.s_id, x.s_name)))
      .map(x => (x._2._2.get, x._2._1._1, x._2._1._2))
    //      .foreach(println)

    no_print(37)
    //    37、查询课程不及格的学生
    //    val student: Array[String] = rdd_score.filter(_.s_score < 60).map(_.s_id).distinct().collect()
    //    rdd_student.filter(x=>student.contains(x.s_id)).foreach(println)

    no_print(38)
    //    查询课程编号为01且课程成绩在80分以上的学生的学号和姓名
    val student: Array[String] = rdd_score.filter(x => (x.s_score > 80 && x.c_id == "01")).map(_.s_id).distinct().collect()
    //    rdd_student.filter(x=>student.contains(x.s_id)).foreach(println)

    no_print(39)
    //    39、求每门课程的学生人数
    rdd_score.map(x => (x.c_id, 1)).reduceByKey(_ + _)
      .join(rdd_course.map(x => (x.c_id, x.c_name)))
      .map(x => (x._1, x._2._2, x._2._1))
    //      .foreach(println)

    no_print(40)
    //    40、查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩
    val tid = rdd_teacher.filter(_.t_name == "张三").map(_.t_id).collect()(0)
    val cid: String = rdd_course.filter(_.t_id == tid).map(_.c_id).collect()(0)
    val (sid, score) = rdd_score.filter(_.c_id == cid).map(x => (x.s_id, x.s_score)).sortBy(x => x._2 > x._2, false, 1).take(1)(0)
    rdd_student.filter(_.s_id == sid).map(x => (x.s_id, x.s_name, x.s_birth, x.s_sex, score)).foreach(println)


  }
}

class compare_2(val avg_score: Double, val cid: String) extends Ordered[compare_2] with Serializable {
  override def compare(that: compare_2): Int = {
    if (this.avg_score - that.avg_score != 0) {
      if (this.avg_score - that.avg_score > 0) { //设置为升序
        1
      } else {
        -1
      }
    } else {
      this.cid.compareTo(that.cid) * -1 //设置为降序
    }
  }
}