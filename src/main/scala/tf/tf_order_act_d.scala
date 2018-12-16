package tf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 订单活动
  * 2018/12/02
  *
  * @author zhanghao
  */
object tf_order_act_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("tf_order_act_d")
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

    val plat = spark.sql(
      s"""
         |SELECT day_id,
         |order_id,
         |act_id,
         |charge_fee
         |FROM waimai.ti_order_plat_act_d
         |WHERE day_id='$day_id'
      """.stripMargin)


    val poi = spark.sql(
      s"""
         |SELECT day_id,
         |order_id,
         |act_id,
         |charge_fee
         |FROM waimai.ti_order_poi_act_d
         |WHERE day_id='$day_id'
      """.stripMargin)

    val actInfo = spark.sql(
      s"""
         |SELECT act_type_id,
         |act_type_name,
         |act_id,
         |act_name
         |FROM waimai.td_activity_info
      """.stripMargin)

    val act = plat.union(poi).join(actInfo, Seq("act_id"), "left_outer").distinct()

    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions.pernoden", "1000")

    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.tf_order_act_d(
        |order_id int,
        |act_type_id int,
        |act_type_name string,
        |act_id int,
        |act_name string,
        |charge_fee double
        |)
        |COMMENT '订单活动'
        |PARTITIONED BY (day_id string)
        |STORED AS orc
      """.stripMargin

    println(createTable)
    spark.sql(createTable)
    act.createOrReplaceTempView("act")
    //保存数据到目标表中
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE waimai.tf_order_act_d partition(day_id)
         |SELECT order_id,
         |act_type_id,
         |act_type_name,
         |act_id,
         |act_name,
         |charge_fee,
         |day_id
         |FROM act
         |WHERE day_id='$day_id'
      """.stripMargin)


    spark.stop()

  }
}
