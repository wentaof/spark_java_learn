package com.learn.spark_core.school.code

import org.apache.spark.rdd.RDD

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/11 10:15
  * @Version 1.0.0
  * @Description TODO
  */
object sql_20_30 extends main_object {
  def main(args: Array[String]): Unit = {
    no_print(21)
//    21、查询不同老师所教不同课程平均分从高到低显示
    rdd_score.groupBy(_.c_id)
      .map(x=>{
        val cid = x._1
        val scores = x._2.map(_.s_score).toList

        val cnt = scores.size
        val total = scores.sum

        val avg_score = total*1.0 /cnt
        ( avg_score,cid)
      })
      .sortByKey(false,1)
//      .foreach(println)


    no_print(22)
//        22、查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
    val id_sid_score_cid = rdd_score.groupBy(_.c_id)
      .flatMap(x => {
        val cid = x._1
        val sid_score = x._2.toList.map(x => (x.s_id, x.s_score,cid)).toList.sortWith(_._2 > _._2)
        val ids = (1 to sid_score.size).toList
        val id_sid_score = ids.zip(sid_score)
        id_sid_score
      })
      .filter(x=>(x._1 >=2 && x._1 <=3))
      .map(x=>(x._2._1,(x._2._3,x._2._2))) //(sid,(cid,score))
      .join(rdd_student.map(x=>(x.s_id,(x.s_name,x.s_birth,x.s_sex))))
//      cid,sid,score,x.s_name,x.s_birth,x.s_sex
      .map(x=>(x._2._1._1,x._1,x._2._1._2, x._2._2._1,x._2._2._2,x._2._2._3))
      .sortBy(_._1,true,1)
//      .foreach(println)

    no_print(23)
//    23、统计各科成绩各分数段人数：课程编号,课程名称,[100-85],[85-70],[70-60],[0-60]及所占百分比
    no_print(24)
//    24、查询学生平均成绩及其名次
    val sid_avgscore: List[(String, Double)] = rdd_score.groupBy(_.s_id).map(x => {
      val sid = x._1
      val scores = x._2.toList.map(_.s_score)
      val cnt = scores.size
      val total = scores.sum
      val avg_score = total * 1.0 / cnt
      (sid, avg_score)
    })
      .collect()
      .toList
      .sortWith(_._2 > _._2)
    val cnt = sid_avgscore.size
    (1 to cnt).toList.zip(sid_avgscore)
//      .foreach(println)

    no_print(25)
//    25、查询各科成绩前三名的记录
    rdd_score.map(x=>(x.c_id,x.s_score)    )
      .groupByKey(1)
      .flatMap(x=>{
        val cid = x._1
        val scores = x._2.toList.sortWith(_ > _).map((_,cid))
        val ids = (1 to scores.size).toList
        val ids_score = ids.zip(scores)
          ids_score
      })
      .filter(_._1 < 4)
//      .foreach(println)

no_print(26)
//    26、查询每门课程被选修的学生数
//    rdd_score.map(x=>(x.c_id,x.s_id)).countByKey().foreach(println)
    no_print(27)
//    27、查询出只有两门课程的全部学生的学号和姓名
    val stu_2: List[String] = rdd_score.map(x=>(x.s_id, x.c_id)).countByKey().filter(_._2 == 2).map(_._1).toList
//    rdd_student.filter(x=>stu_2.contains(x.s_id)).foreach(println)

    no_print(28)
//    28、查询男生、女生人数
//    rdd_student.map(x=>(x.s_sex,x.s_name)).countByKey().foreach(println)

    no_print(29)
//    29、查询名字中含有"风"字的学生信息
//    rdd_student.filter(_.s_name.contains("风")).foreach(println)

    no_print(30)
//    30、查询同名同性学生名单，并统计同名人数
    rdd_student.map(x=>(x.s_name,1)).reduceByKey(_+_)
      .filter(_._2 > 1)
      .foreach(println)



  }
}
