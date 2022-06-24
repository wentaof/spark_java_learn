package com.learn.spark_streaming.project.utils

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/20 14:18
  * @Version 1.0.0
  * @Description TODO
  */
//timestamp + " " + area + " " + city + " " + userid + " " + adid
case class click_info(day: String, area: String, city: String, userid: Int, adid: Int)