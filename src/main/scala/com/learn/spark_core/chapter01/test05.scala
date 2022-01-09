package com.learn.spark_core.chapter01

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/5 20:52
  * @Version 1.0.0
  * @Description TODO
  */
object test05 {
  def main(args: Array[String]): Unit = {
    val names = List("fengwentao","liubobo")
    val p = person()
    names.map(name=> p.sayBye(p.sayHi(name))).foreach(println)
    println("_______________________")
    names.map( p.sayHi _ andThen p.sayBye).foreach(println)
    println("_______________________")
    names.map( p.sayHi _ compose p.sayBye).foreach(println)
  }
}

case class person(){
  var name:String = _
  def sayHi(name: String) = "Hi, " + name
  def sayBye(str: String) = str + ", bye"
}