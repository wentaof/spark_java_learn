package com.learn.spark_core.school.bean

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/9 17:42
  * @Version 1.0.0
  * @Description TODO
  */
case class Student(
                    s_id: String,
                    s_name: String,
                    s_birth: String,
                    s_sex: String
                  )

case class Course(
                   c_id: String,
                   c_name: String,
                   t_id: String
                 )

case class Teacher(
                    t_id: String,
                    t_name: String
                  )

case class Score(
                  s_id: String,
                  c_id: String,
                  s_score: Int
                )