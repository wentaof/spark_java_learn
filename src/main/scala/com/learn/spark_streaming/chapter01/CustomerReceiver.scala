package com.learn.spark_streaming.chapter01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/17 20:14
  * @Version 1.0.0
  * @Description 自定义数据源
  *             实现监控某个端口号,获取该端口号的内容
  */
class CustomerReceiver(host: String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    new Thread("Socket Recever"){
      override def run(): Unit ={
        receive()
      }
    }.start()
  }

  def receive()={
    val socket = new Socket(host,port)
    var input:String = null
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
    input = reader.readLine()
    while (!isStopped() && input != null){
      store(input)
      input = reader.readLine()
    }
    reader.close()
    socket.close()
    restart("restart")
  }

  override def onStop(): Unit = {}
}
