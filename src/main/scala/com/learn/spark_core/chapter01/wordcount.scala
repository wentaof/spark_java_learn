package com.learn.spark_core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/4 16:31
  * @Version 1.0.0
  * @Description TODO
  */
object wordcount {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("worldcount")
    val sc = new SparkContext(sparkconf)
    //    val file_rdd: RDD[String] = sc.textFile("D:\\desktop\\learn\\spark\\spark-core\\wordcount.txt")
    //    val file_rdd_flatmap: RDD[String] = file_rdd.flatMap(x => x.split(" "))
    ////    val str: String = file_rdd_flatmap.collect().mkString(",")
    ////    println(str)
    //    file_rdd_flatmap.map(x => (x,1))
    //      .reduceByKey((x,y) => x+y)
    //      .collect()
    //      .foreach(println)


    //    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6))
    //    val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    //    rdd1.collect().foreach(println)
    //    rdd2.collect().foreach(println)

    //      val rdd1: RDD[String] = sc.textFile("D:\\desktop\\learn\\spark\\spark-core\\wordcount.txt",8)

    //      rdd1.mapPartitionsWithIndex((index,i) =>{
    //        var a:Iterator[Int] = null
    //        if(index == 1){
    //          a = List(1,2,3).toIterator
    //        }else{
    //          a=List().toIterator
    //        }
    //        a
    //      })

    //    println(rdd1.foreach(println))
    //    rdd1.glom().map(_.mkString(",")).foreach(println)

    //    println(rdd2.partitions.size)

    //    val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4),1)
    //    rdd2.groupBy(_ % 2).foreach(x => println(x._1+"--->"+x._2.toString()))
    //    rdd2.groupBy(_ % 2).foreach(x,y =>{
    //      println(x,y)
    //    } )
    //    println(1,2)
    //  val dataRDD = sc.makeRDD(List(    1,2,3,4  ),1)
    //    val dataRDD1 = dataRDD.sample(false, 0.5)
    //    println(dataRDD1.collect().mkString(","))
    //    val dataRDD2 = dataRDD.sample(true, 2)
    //    println(dataRDD2.collect().mkString(","))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2), 1)
    //    println(rdd.partitions.size)
    //    println(rdd.distinct().partitions.size)

    var rdd1 = rdd.distinct(2)
    rdd1.cache()
    rdd1.foreachPartition(x=>{
      for (elem <- x) {
        println(elem)
      }
    })
    println("------------")
    rdd1.repartition(1).foreach(println(_))


  }
}
