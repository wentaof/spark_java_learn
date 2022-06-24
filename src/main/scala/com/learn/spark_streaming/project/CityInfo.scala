package com.learn.spark_streaming.project

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/18 20:47
  * @Version 1.0.0
  * @Description TODO
  * @param city_id 城市 id
  * @param city_name 城市名称
  * @param area 城市所在大区
  */
case class CityInfo(
                     city_id:Long,
                     city_name:String,
                     area:String
                   )
