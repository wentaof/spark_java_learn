package com.learn.spark_core.school.code

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/10 18:36
  * @Version 1.0.0
  * @Description TODO
  */
import org.apache.spark.rdd.RDD

import scala.collection.mutable._
import scala.math.Ordering.Int

object sql_10_20 extends main_object {
  def main(args: Array[String]): Unit = {
    no_print(11)
//    val total_course_cnt = rdd_course.count()
//    val sids: List[String] = rdd_score.map(x=>(x.s_id,1)).countByKey().filter(_._2 == total_course_cnt).map(_._1).toList
//    rdd_student.filter(x=> !sids.contains(x.s_id)).foreach(println)

    no_print(12)
//    查询至少有一门课与学号为"01"的同学所学相同的同学的信息
//    val s01_cids= rdd_score.filter(_.s_id == "01").map(_.c_id).collect()
//    val sids = rdd_score.filter(_.s_id != "01").filter(x=>s01_cids.contains(x.c_id)).map(_.s_id).distinct().collect()
//    rdd_student.filter(x=>sids.contains(x.s_id)).foreach(println)

    no_print(13)
//    13、查询和"01"号的同学学习的课程完全相同的其他同学的信息
//    val s01_cids: List[String] = rdd_score.filter(_.s_id == "01").map(_.c_id).toLocalIterator.toList
//    val sids: Array[String] = rdd_score.groupBy(_.s_id)
//      .map(x => {
//        val sid = x._1
//        val cids = ListBuffer[String]()
//        for (elem <- x._2) {
//          cids.append(elem.c_id)
//        }
//        (sid, cids.toList)
//      })
//      .filter(s01_cids == _._2)
//      .map(_._1)
//      .collect()
//    rdd_student.filter(x=>sids.contains(x.s_id)).foreach(println)

    no_print(14)
//    14、查询没学过"张三"老师讲授的任一门课程的学生姓名
//    val cids: Array[String] = rdd_course.map(x => (x.t_id, x.c_id)).join(rdd_teacher.map(x => (x.t_id, x.t_name)))
//      .filter(_._2._2 == "张三")
//      .map(_._2._1)
//      .distinct().collect()
//    val sids: Array[String] = rdd_score.map(x=>(x.s_id,x.c_id)).filter(x=>{cids.contains(x._2)} ).map(_._1).distinct().collect()
//    rdd_student.filter(x=> ! sids.contains(x.s_id)).foreach(println)

    no_print(15)
//    15、查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
//    val sid_avgscore = rdd_score.groupBy(_.s_id)
//      .map { case (s_id, scores) => {
//        val cnt = scores.size
//        val total_score = scores.map(_.s_score).toList.sum
//        val avg_score = total_score * 1.0 / cnt
//        val down60_cnt = scores.map(_.s_score).filter(_ < 60).size
//        (s_id, avg_score,down60_cnt)
//      }
//      }.filter(_._3 > 1)
//      .map(x=>(x._1,x._2))
//    rdd_student.map(x=>(x.s_id,(x.s_name))).join(sid_avgscore)
//      .map(x=>(x._1,x._2._1,x._2._2))
//      .foreach(println)

    no_print(16)
//    16、检索"01"课程分数小于60，按分数降序排列的学生信息
//    sortByKey数据类型为k-v，且是按照key进行排序。sortByKey是局部排序，不是全局排序，如果要进行全局排序，必须将所有的数据都拉取到一台机器上面才可以
//    sortBy其实是使用sortByKey来实现，但是比sortByKey更加灵活，因为sortByKey只能应用在k-v数据格式上，而这个sortBy可以用在非k-v键值对的数据格式上面。
//    val sid_score = rdd_score.filter(x=>(x.c_id=="01" && x.s_score < 60))
//      .map(x=>(x.s_id,x.s_score))
//
//    rdd_student.map(x=>(x.s_id,(x.s_name,x.s_sex,x.s_birth)))
//      .join(sid_score)
//      .sortBy(_._2._2,false,1)
//      .foreach(println)

    no_print(17)
//    17、按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
//    rdd_score.groupBy(_.s_id)
//      .map(x=>{
//        val sid = x._1
//        val scores = x._2.toList.map(_.s_score)
//        val sum = scores.sum
//        val cnt = scores.size
//        val avg_score = sum * 1.0 /cnt
//        (sid,(sum,avg_score))
//      })
//      .join(rdd_student.map(x=>(x.s_id,x.s_name)))
//      .sortBy(_._2._1._2,false,1)
//      .foreach(println)

no_print(18)
//    18.查询各科成绩最高分、最低分和平均分：以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率
//	–及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
//    val score_info: RDD[(String, (Int, Int, Double, Double, Double, Double, Double))] = rdd_score.groupBy(_.c_id)
//      .map(x => {
//        val cid = x._1
//        val scores = x._2.toList.map(_.s_score)
//        val score_max = scores.max
//        val score_min = scores.min
//        val cnt = scores.size
//        val sum = scores.sum
//        val score_avg = sum * 1.0 / cnt
//        val jige = scores.filter(_ >= 60).size * 1.0
//        val zhongdeng = scores.filter(x => x >= 70 && x < 80).size * 1.0
//        val youliang = scores.filter(x => x >= 80 && x < 90).size * 1.0
//        val youxiu = scores.filter(x => x >= 90).size * 1.0
//        (cid, (score_max, score_min, score_avg, jige/cnt, zhongdeng/cnt, youliang/cnt, youxiu/cnt))
//      })
//    rdd_course.map(x=>(x.c_id,x.c_name))
//        .join(score_info)
//        .foreach(println)

    no_print(19)
    //19、按各科成绩进行排序，并显示排名
      //	-row_number() over()分组排序功能(mysql没有该方法) 这个是顺序的1，2，3，4
      //  -rank() over() 这个是阶跃的 1，2，2，4
      //  -dense_rank() over() 这个是顺序连续的 1，2，2，3
    val id_score_sid: RDD[(Int, (Int, String,String))] = rdd_score.groupBy(_.c_id)
      .flatMap(x => {
        val cid = x._1
        val scores = x._2.map(x => (x.s_score, x.s_id, cid)).toList
        val score_sorted = scores.sortWith(_._1 > _._1)
        val ids = (1 to score_sorted.size).toList
        // (排名,(分数,sid,cid))
        val ids_score = ids.zip(score_sorted)
        ids_score
      })
    id_score_sid.map(x=>(x._2._2,(x._1,x._2._1,x._2._3)))
      .join(rdd_student.map(x=>(x.s_id,x.s_name)))
      .map(x=>(x._1,x._2._2,x._2._1._1,x._2._1._2,x._2._1._3))
      .sortBy(x=>(x._5,x._3),true,1)
      .map(x=>(x._5,x._3,x._1,x._2,x._4))
      .foreach(println)

no_print(20)
//    查询学生的总成绩并进行排名
//    val result = rdd_score.groupBy(_.s_id)
//      .map(x => {
//        val sid = x._1
//        val sum = x._2.map(_.s_score).toList.sum
//        (sid, sum)
//      }).sortBy(_._2,false,1)
//      .collect()
//    val ids = (1 to result.size).toList
//
//    val aaa: RDD[(Int, (String, Int))] = sc.parallelize(
//      ids.zip(result)
//    )
//    aaa.sortBy(_._1,true,1).foreach(println)



  }
}
