package com.learn.spark_core.school.code


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.learn.spark_core.school.bean.Score
import com.twitter.chill.java.SimpleDateFormatSerializer

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/13 16:19
  * @Version 1.0.0
  * @Description TODO
  */
object sql_40_50 extends main_object {
  def main(args: Array[String]): Unit = {
    no_print(41)
//    查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
    rdd_score.groupBy(_.s_score).filter(_._2.size > 1)
      .flatMap(_._2)
//      .foreach(println)

    no_print(42)
//    42、查询每门课程成绩最好的前三名
    rdd_score.groupBy(_.c_id)
      .flatMap(x=>{
        val scores: List[Score] = x._2.toList.sortWith(_.s_score > _.s_score)
        val ids = List(1,2,3)
        val tuples: List[(Int, Score)] = ids.zip(scores)
        tuples
      })
      .sortBy(_._2.c_id,true,1)
//      .foreach(println)

    no_print(43)
//43、统计每门课程的学生选修人数（超过5人的课程才统计）
    //	-要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列
    rdd_score.groupBy(_.c_id).filter(_._2.size > 5)
      .map(x=>(x._1,x._2.size))
//      .foreach(println)
      .map(x=>{
      (new compare_v2_1(x._1,x._2),x)
    })
      .sortByKey(numPartitions = 1)
      .map(_._2)
//      .foreach(println)


    no_print(44)
//    44、检索至少选修两门课程的学生学号
    val sids: Array[String] = rdd_score.groupBy(_.s_id).filter(_._2.size > 2).map(_._1).collect()
//    rdd_student.filter(x=>sids.contains(x.s_id)).foreach(println)

    no_print(45)
//    45、查询选修了全部课程的学生信息
    val cids = rdd_score.groupBy(_.c_id).map(_._1).collect().size
    val sids_45: Array[String] = rdd_score.groupBy(_.s_id).filter(_._2.size == cids).map(_._1).collect()
//    rdd_student.filter(x=>sids_45.contains(x.s_id)).foreach(println)

    no_print(46)
//    46、查询各学生的年龄(周岁)
    //	-按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val format2 = new SimpleDateFormat("MM-dd")
    rdd_student.map(x=>{
      val name = x.s_name
      val birth = format.parse(x.s_birth)
      val today = format2.format(new Date())
      val birth_day = format2.format(birth)
      val age = if (today.compareTo(birth_day)>0){
        2022 - 1- format.parse(x.s_birth).getYear - 1900
      }else{
        2022 - format.parse(x.s_birth).getYear - 1900
      }
      (name,age)
    })
//      .foreach(println)


    no_print(47)
//    47、查询本周过生日的学生
    val start_time = getCurrentWeekStartDate()
    val endt_ime = getCurrentWeekEndDate()
    rdd_student.filter(x=>{
      val birth = x.s_birth.substring(5,10)
      val startday: String = format2.format(start_time)
      val endtime: String = format2.format(endt_ime)
      startday.compareTo(birth) < 0 && endtime.compareTo(birth) >= 0
    })
      .foreach(println)




  }

  def getCurrentWeekStartDate() ={
    val cal: Calendar = Calendar.getInstance()
    val currentday = cal.get(Calendar.DAY_OF_WEEK)
    var preday:Int = 0
    if(currentday == 1){
      preday = -6
    }else{
      preday = 2 - currentday
    }

    cal.add(Calendar.DAY_OF_WEEK,preday)
    cal.set(Calendar.HOUR_OF_DAY,0)
    cal.set(Calendar.MINUTE,0)
    cal.set(Calendar.SECOND,0)
    cal.set(Calendar.MILLISECOND,0)

    cal.getTime
  }

  def getCurrentWeekEndDate() ={
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(getCurrentWeekStartDate())
    cal.add(Calendar.DAY_OF_WEEK,6)
    cal.set(Calendar.HOUR_OF_DAY,23)
    cal.set(Calendar.MINUTE,59)
    cal.set(Calendar.SECOND,59)
    cal.set(Calendar.MILLISECOND,999)

    cal.getTime

  }

}

class compare_v2_1(val cid:String, val cnt:Int) extends Ordered[compare_v2_1] with Serializable {
  override def compare(that: compare_v2_1): Int = {
    if (this.cnt - that.cnt != 0){
      if(this.cnt - that.cnt > 0){
        -1
      }else{
        1
      }
    }else{
      this.cid.compareTo(that.cid)
    }
  }
}
