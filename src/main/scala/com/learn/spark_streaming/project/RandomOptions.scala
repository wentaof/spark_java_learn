package com.learn.spark_streaming.project

import scala.util.Random

import scala.collection.mutable.ListBuffer

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/18 20:48
  * @Version 1.0.0
  * @Description TODO
  */
case class RanOpt[T](value: T, weight: Int)

object RandomOptions {
  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }
}

class RandomOptions[T](opts: RanOpt[T]*) {
  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)  //根据index获取对应位置上的元素
  }
}

