package com.learn.spark_core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/5 13:40
  * @Version 1.0.0
  * @Description TODO
  */
object test02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02")
    val sc = new SparkContext(conf)
    //    val rdd = sc.makeRDD(Range(1,10),2)
    val rdd2 = sc.makeRDD(List(1, 8, 2, 9, 7, 4, 3, 5, 6), 3)
    val rdd3 = sc.makeRDD(List(1, 2, 3, 4, 5, 11, 12, 13, 14), 2)
    val rdd_str = sc.makeRDD(List("1", "2", "3", "4", "5", "11", "12", "13", "14"), 2)

    //    rdd.foreach(println)

    //    rdd
    //      //也是分区内有序,分区间无序.如果需要将整个进行排序,可以设置sortby的分区数量为1
    //      .sortBy(num => num,false,5)
    //      .foreachPartition(x => println(x.mkString(",")))
    //两个rdd分区数不一样,也可以统计出来,结果分区数= max(rdd1.partitions,rdd2.partitions)

    //    //数据类型必须一致,否则报错
    //    rdd2.intersection(rdd3).foreachPartition(x => println(x.mkString(",")))
    // union时 分区也会合并, rdd_union的分区数为5
    //    val rdd_union: RDD[Int] = rdd2.union(rdd3)
    //    print_rdd(rdd_union)

    //结果rdd的分区数量= 前面被减的rdd数量
    val rdd_substract: RDD[Int] = rdd3.subtract(rdd2)
    //    print_rdd(rdd_substract)

    // zip 拉链操作
    //    思考一个问题：如果两个 RDD 数据类型不一致怎么办？
    //    数据类型可以不一致
    //    思考一个问题：如果两个 RDD 数据分区不一致怎么办？
    //    rdd的每个分区内数据量应该一致
    //    思考一个问题：如果两个 RDD 分区数据数量不一致怎么办
    //        分区数量必须一致
    //    print_rdd(rdd2)
    //    print_rdd(rdd3)
    val rdd_zip = rdd2.zip(rdd_str)
    //    思考一个问题：如果重分区的分区器和当前 RDD 的分区器一样怎么办？
    //      也会重新分区,分区内的数据会发生改变
    //    思考一个问题：Spark 还有其他分区器吗？
    //      HashPartitioner, RangePartitioner, 自定义分区器
    //    思考一个问题：如果想按照自己的方法进行数据分区怎么办？
    //自定义的Hash分区器
    //class CustomPartitioner(numPar: Int) extends Partitioner {
    //  assert(numPar > 0)
    //
    //  // 返回分区数, 必须要大于0.
    //  override def numPartitions: Int = numPar
    //
    //  //返回指定键的分区编号(0到numPartitions-1)
    //  override def getPartition(key: Any): Int = key match {
    //    case null => 0
    //    case _ => key.hashCode().abs % numPar
    //  }
    //}

    //    print_rdd(rdd_zip)
    //    val rdd_partition: RDD[(Int, String)] = rdd_zip.partitionBy(new HashPartitioner(2))
    //    print_rdd(rdd_partition)

    //    val rdd_reducebykey= rdd_zip.union(rdd_zip).groupByKey(new HashPartitioner(2))
    //    print_rdd(rdd_reducebykey)

    //    val rdd_agg: RDD[(Int, String)] = rdd_zip.union(rdd_zip).aggregateByKey("hello")((x,y) =>x+y,(i,j)=>i+"-"+j)
    //    val rdd_agg: RDD[(Int, String)] = rdd_zip.union(rdd_zip).foldByKey("hello")((i,j)=>i+"-"+j)
    //    print_rdd(rdd_agg)
    // 结果:
    //    (3,hello-12-hello-12),(7,hello-5-hello-5)
    //    (4,hello-11-hello-11),(8,hello-2-hello-2)
    //    (6,hello-14-hello-14),(2,hello-3-hello-3)
    //    (1,hello-1-hello-1),(9,hello-4-hello-4),(5,hello-13-hello-13)

    val l = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98), ("b", 98))
    val rdd_list = sc.makeRDD(l, 2)
    // 下面的v 就相当于key对应的值, 当有大于等于2个的时候才能走进去聚合,要不用聚合

    //    val rdd_combine = rdd_list.combineByKey(
    //      (_, 1), //createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
    //      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
    //      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) //mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
    //    )   //
    ////        .map(x =>{          (x._1,x._2._1 / x._2._2)        })
    //      //下面这个map必须使用{},而不能使用(), {}会启用模式匹配,()则不会启用
    //      .map{case (name,(score,num)) => (name,score/num)}
    //      .collect()
    //
    //    rdd_combine.foreach(println)

    //    print_rdd(rdd_combine)


    //    print_rdd(rdd_list)
    //    val rdd_sortbykey: RDD[(String, Int)] = rdd_list.sortByKey()
    //    print_rdd(rdd_sortbykey)


    //    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")))
    //    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6),(5,7)))
    ////    rdd.join(rdd1).collect().foreach(println)
    ////    rdd.rightOuterJoin(rdd1).collect().foreach(println)
    //    rdd.fullOuterJoin(rdd1).collect().foreach(println)
    /*
        val dataRDD1 = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
        val dataRDD2 = sc.makeRDD(List(("a", 1), ("c", 2), ("c", 3)))
        val rdd_cogroup = dataRDD1.cogroup(dataRDD2)
        print_rdd(rdd_cogroup)
    */


    //    Action 算子
    //    val dataRDD1 = sc.makeRDD(Range(1,20),6)
    //    print_rdd(dataRDD1)
    //    dataRDD1.take(10).foreach(println)

    //    rdd2.takeOrdered(4).foreach(println)
    //    print_rdd(rdd2)
    ////    println(rdd2.aggregate(2)((x,y)=> x + y,(x,y)=>x * y))
    //    println(rdd2.fold(2)(_+_))

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2,
      "b"), (3, "c"), (3, "c")))
    // 统计每种 key 的个数
    val result: collection.Map[Int, Long] = rdd.countByKey()
    println(result)
    sc.stop()
  }

  def print_rdd[T <: Any](rdd: RDD[T]) = {
    rdd.foreachPartition(x => println(x.mkString(",")))
  }

}
