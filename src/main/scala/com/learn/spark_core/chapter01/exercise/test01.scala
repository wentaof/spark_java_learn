package com.learn.spark_core.chapter01.exercise

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/9 11:16
  * @Version 1.0.0
  * @Description TODO
  */
import scala.collection.mutable._
object test01 {
  def main(args: Array[String]): Unit = {
    val a = ListBuffer[String]()
    a.++=(List("1","2"))
    a.++=(List("1","2"))
    println(a)
  }
}
