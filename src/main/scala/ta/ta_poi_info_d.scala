package ta

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 商家数据概览
  * 2018/12/02
  *
  * @author zhanghao
  */
object ta_poi_info_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ta_poi_info_d")
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
    import spark.sql

    //根据tm层生成商家相关数据，并且限制订单为完成（status=9）
    val poi = sql(
      s"""
         |SELECT
         |a.poi_id,
         |a.poi_name,
         |a.phone,
         |a.address,
         |count(order_id) as order_num_d,
         |count(distinct a.user_id) as user_num,
         |round(sum(a.poi_charge_fee),2) as poi_charge_fee,
         |round(sum(a.fee),2) as original_fee,
         |round(sum(a.fee-a.shipping_fee-a.poi_charge_fee),2) as actual_fee,
         |a.day_id,
         |b.week_id,
         |b.month_id
         |FROM waimai.tm_order_detail_d a
         |LEFT OUTER JOIN waimai.td_date_info b
         |ON a.day_id=b.day_id
         |WHERE a.day_id='$day_id'
         |AND a.status=9
         |GROUP BY a.poi_id,
         |a.poi_name,
         |a.phone,
         |a.address,
         |a.day_id,
         |b.week_id,
         |b.month_id
      """.stripMargin)
    poi.createOrReplaceTempView("poi")
    poi.show(5)

    //使用窗口函数计算本周、本月、全部订单量
    val target = spark.sql(
      """
        |SELECT
        |poi_id,
        |poi_name,
        |phone,
        |address,
        |order_num_d,
        |sum(order_num_d) over (partition by poi_id,week_id ORDER BY day_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_num_w,
        |sum(order_num_d) over (partition by poi_id,month_id ORDER BY day_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_num_m,
        |sum(order_num_d) over (partition by poi_id ORDER BY day_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_num_all,
        |user_num,
        |poi_charge_fee,
        |original_fee,
        |actual_fee,
        |day_id
        |FROM poi
      """.stripMargin)
    target.show(5)
    target.createOrReplaceTempView("target")

    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.ta_poi_info_d(
        |poi_id int,
        |poi_name string,
        |phone string,
        |address string,
        |order_num_all bigint,
        |order_num_d bigint,
        |order_num_w bigint,
        |order_num_m bigint,
        |user_num bigint,
        |poi_charge_fee double,
        |original_fee double,
        |actual_fee double)
        |COMMENT '订单活动'
        |PARTITIONED BY (day_id string)
        |STORED AS orc
      """.stripMargin
    println(createTable)
    sql(createTable)


    //设置hive的相关参数
    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions.pernoden", "1000")
    //保存数据到目标表中

    sql(
      """
        |INSERT OVERWRITE TABLE waimai.ta_poi_info_d partition(day_id)
        |SELECT poi_id,
        |poi_name,
        |phone,
        |address,
        |order_num_all,
        |order_num_d,
        |order_num_w,
        |order_num_m,
        |user_num,
        |poi_charge_fee,
        |original_fee,
        |actual_fee,
        |day_id
        |FROM target
      """.stripMargin)

    spark.stop()

  }
}
