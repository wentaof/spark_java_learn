package com.learn.spark_streaming.project.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql
import org.apache.spark.sql.execution.datasources.DataSource

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/19 15:38
  * @Version 1.0.0
  * @Description TODO
  */

object mysql_utils {

  //定义数据源
  var source: sql.DataSource = init()
//初始化数据源
  def init()={
    val prop = new Properties()
    prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("mysql_config.properties"))
    DruidDataSourceFactory.createDataSource(prop)
  }

  def getconnection() = {
    source.getConnection
  }

  def getDataSource() = {
    source
  }


  def upsert_execute(sql:String,conn:Connection): Int ={
    val stm: PreparedStatement = conn.prepareStatement(sql)
    var flag = 0
    try{
      flag = stm.executeUpdate()
    }
    catch {
      case e: Exception=> {
        println(e)
        conn.rollback()
      }
    }

    if(stm != null){
      stm.close()
    }
    flag
  }

  def query_execute_long(sql:String,conn:Connection):Long ={
    val stm: PreparedStatement = conn.prepareStatement(sql)
    var rs: ResultSet = null
    try{
       rs = stm.executeQuery(sql)
    }
    catch {
      case e: Exception=> {
        println(e)
      }
    }

    var result:Long = -1
    if(rs.next()){
     result = rs.getLong(1)
    }

    if(stm != null){
      stm.close()
    }

    result
  }

//实时统计:分日期,大区,城市,广告统计点击结果
  def upsert_area_city_ad_count(conn: Connection, elem: ((String, String, String, Int), Int)) = {
    val upsert_sql=
      """
        |insert into area_city_ad_count values(?,?,?,?,?)
        |ON DUPLICATE KEY UPDATE count = count + ?
      """.stripMargin
    val stm: PreparedStatement = conn.prepareStatement(upsert_sql)
    stm.setString(1,elem._1._1)
    stm.setString(2,elem._1._2)
    stm.setString(3,elem._1._3)
    stm.setInt(4,elem._1._4)
    stm.setInt(5,elem._2)
    stm.setInt(6,elem._2)

    stm.executeUpdate()
    stm.close()
  }


  def main(args: Array[String]): Unit = {
    val conn = getconnection()
//    conn.setAutoCommit(false)
//    val sql = "INSERT INTO `spark2020`.`user_ad_count` (`dt`, `userid`, `adid`, `count`) VALUES ('2002-01-01', 1, 1, 0);"
//    val sql = "select userid from black_list where userid = 1"
//    println(query_execute_long(sql,conn))
//    println(query_execute_long(sql,conn))

    upsert_area_city_ad_count(conn,(("2020","a","a",1),1))

  }

}