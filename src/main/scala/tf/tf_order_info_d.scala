package tf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 2018/12/02
  * 订单明细数据
  * 订单基础信息和订单配送信息整合到一起，订单的相关属性都可以从这张表里面取
  * 参数说明：args(0) 传入要执行的日期，不传默认执行昨天
  * 执行命令 spark-submit --class  tf.td_order_info_d  --master local[*] waimai.jar 20180501
  *
  * @author zhanghao
  */
object tf_order_info_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("td_order_info_d")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //处理day_id
    var day_id = ""
    //获取昨天的日期
    val yesterday = LocalDate.now.minus(1, ChronoUnit.DAYS) format DateTimeFormatter.ofPattern("yyyyMMdd")
    //如果没有传参，则默认昨天
    if (args.length < 1) {
      day_id = yesterday
    } else {
      day_id = args(0)
    }
    //读取hive表
    //城市维表
    val city = spark.sql(
      """
        |SELECT province_id,
        |province_name,
        |city_id,
        |city_name
        |FROM waimai.td_city_info
      """.stripMargin)

    //订单基本信息
    val basic = spark.sql(
      s"""
         |SELECT day_id,
         |order_id,
         |city_id,
         |poi_id,
         |user_id,
         |terminal_id,
         |fee,
         |order_time,
         |status,
         |remark
         |FROM waimai.ti_order_delta_d
         |WHERE day_id='$day_id'
      """.stripMargin).join(city, Seq("city_id"), "left_outer")
    basic.show(5)


    //配送维度表
    val del_info = spark.sql(
      """
        |SELECT delivery_id,
        |delivery_name
        |FROM waimai.td_delivery_type_info
      """.stripMargin)

    //订单配送信息
    //订单状态以订单表为准，不从配送表中获取
    //实际生产中有可能两个状态都需要，并且代表含义不一样，需要起别名，在整合层存储为另外一个字段
    val del_order = spark.sql(
      s"""
         |SELECT day_id,
         |order_id,
         |shipping_fee,
         |rider,
         |delivery_type_id,
         |delivery_time
         |--status 以订单表为准
         |FROM waimai.ti_order_delivery_delta_d
         |WHERE day_id='$day_id'
    """.stripMargin
    )

    //关联字段名称不同时，使用$(key)方式来关联
    import spark.implicits._
    val delivery = del_order.join(del_info, $"delivery_type_id" === $"delivery_id", "left_outer")
    delivery.show(5)


    //订单信息 使用day_id和order_id 进行左关联
    val order = basic.join(delivery, Seq("day_id", "order_id"), "left_outer")
    order.show(5)


    //创建临时视图order
    order.createOrReplaceTempView("order")

    //设置hive的相关参数



    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.tf_order_info_d(
        |order_id int,
        |province_id int,
        |province_name string,
        |city_id int,
        |city_name string,
        |poi_id int,
        |user_id int,
        |platform_id int,
        |platform_name string,
        |terminal_id int,
        |terminal_name string,
        |fee double,
        |order_time string,
        |remark string,
        |shipping_fee double,
        |rider string,
        |delivery_type_id int,
        |delivery_name string,
        |delivery_time string,
        |status int
        |)
        |COMMENT '订单明细信息表'
        |PARTITIONED BY(day_id string)
        |STORED AS orc
      """.stripMargin
    println(createTable)
    spark.sql(createTable)

    //保存数据到目标表中
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE  waimai.tf_order_info_d PARTITION(day_id)
         |SELECT order_id,
         |province_id,
         |province_name,
         |city_id,
         |city_name,
         |poi_id,
         |user_id,
         |coalesce(b.platform_id,0),
         |coalesce(b.platform_name,'未知'),
         |a.terminal_id,
         |coalesce(b.terminal_name,'未知'),
         |fee,
         |order_time,
         |remark,
         |shipping_fee,
         |rider,
         |delivery_type_id,
         |delivery_name,
         |delivery_time,
         |status,
         |day_id
         |FROM order a
         |LEFT OUTER JOIN waimai.td_terminal_info b
         |ON a.terminal_id=b.terminal_id
         |WHERE a.day_id='$day_id'
  """.stripMargin)

    spark.stop()


  }
}
