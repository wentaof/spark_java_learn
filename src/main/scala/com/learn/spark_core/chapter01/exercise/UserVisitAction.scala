package com.learn.spark_core.chapter01.exercise

/**
  * @author fengwentao@changjing.ai
  * @date 2022/1/6 15:37
  * @Version 1.0.0
  * @Description TODO
  */
//用户访问动作表
case class UserVisitAction(
  date: String, //用户点击行为的日期 0
  user_id: Long, //用户的 ID 1
  session_id: String, //Session 的 ID 2
  page_id: Long, //某个页面的 ID 3
  action_time: String, //动作的时间点 4
  search_keyword: String, //用户搜索的关键词 5
  click_category_id: Long, //某一个商品品类的 ID 6
  click_product_id: Long, //某一个商品的 ID 7
  order_category_ids: String, //一次订单中所有品类的 ID 集合 8
  order_product_ids: String, //一次订单中所有商品的 ID 集合 9
  pay_category_ids: String, //一次支付中所有品类的 ID 集合 10
  pay_product_ids: String, //一次支付中所有商品的 ID 集合 11
  city_id: Long //城市 id 12
)