package com.learn.spark_streaming.project

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/18 20:43
  * @Version 1.0.0
  * @Description TODO
  */

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}